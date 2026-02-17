/*
 * Copyright 2020 - 2026 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Ivan Muratov (binakot@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.traccar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.traccar.BaseProtocolDecoder;
import org.traccar.helper.BitUtil;
import org.traccar.session.DeviceSession;
import org.traccar.NetworkMessage;
import org.traccar.Protocol;
import org.traccar.helper.Checksum;
import org.traccar.model.Position;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class ArnaviBinaryProtocolDecoder extends BaseProtocolDecoder {

    private static final byte HEADER_START_SIGN = (byte) 0xff;
    private static final byte HEADER_VERSION_1 = 0x22;
    private static final byte HEADER_VERSION_2 = 0x23;

    private static final byte RECORD_DATA = 0x01;
    private static final byte PACKAGE_START = 0x5B;
    private static final byte PACKAGE_END = 0x5D;

    public ArnaviBinaryProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    private void sendResponse(Channel channel, byte version, int index) {
        if (channel != null) {
            ByteBuf response = Unpooled.buffer();
            response.writeByte(0x7b);
            if (version == HEADER_VERSION_1) {
                response.writeByte(0x00);
                response.writeByte((byte) index);
            } else if (version == HEADER_VERSION_2) {
                response.writeByte(0x04);
                response.writeByte(0x00);
                ByteBuffer timeBuf = ByteBuffer.allocate(4);
                // time.writeBytes(time); буфер сам себя записывает
                timeBuf.putInt((int) (System.currentTimeMillis() / 1000)); //исправленный таймкод
                timeBuf.position(0);
                response.writeByte(Checksum.modulo256(timeBuf.slice())); //контрольная сумма
                response.writeBytes(timeBuf);
            }
            response.writeByte(0x7d);
            channel.writeAndFlush(new NetworkMessage(response, channel.remoteAddress()));
        }
    }

    private Position decodePosition(DeviceSession deviceSession, ByteBuf buf, int length, Date time) {
        final Position position = new Position();
        position.setProtocol(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());
        position.setTime(time);

        int startIndex = buf.readerIndex();
        int bytesRead = 0;

        while (bytesRead +5 <= length) { //структура пакета может отличаться, проверяем длину
            int tag = buf.readUnsignedByte();
            int rawValue = buf.readIntLE();
            bytesRead += 5;

            switch (tag) {
                case 1: {
                    int externalVoltage = (rawValue >> 16) & 0xFFFF;
                    int internalVoltage = rawValue & 0xFFFF;
                
                    if (externalVoltage != 0xFFFF && externalVoltage != 0) {
                    position.set(Position.KEY_POWER, externalVoltage / 1000.0);
                    }
                    if (internalVoltage != 0xFFFF && internalVoltage != 0) {
                    position.set(Position.KEY_BATTERY, internalVoltage / 1000.0);
                    }
                    break;
                }
                case 3: {
                    float latitude = Float.intBitsToFloat(rawValue);
                    //Arnavi присылает координаты как 4 байта в виде int, 
                    //которые нужно интерпретировать как IEEE 754 float через intBitsToFloat
                    if (latitude >= -90 && latitude <= 90) { //валидация координат
                        position.setLatitude(latitude);
                        position.setValid(true);
                    }
                    break;
                }
                case 4: {
                    float longitude = Float.intBitsToFloat(rawValue);
                    if (longitude >= -180 && longitude <= 180) { //валидация координат
                        position.setLongitude(longitude);
                        position.setValid(true);
                    }
                    break;
                }
                case 5: {
                    int speedKnots = (rawValue >> 24) & 0xFF;
                    int satByte = (rawValue >> 16) & 0xFF;
                    int altByte = (rawValue >> 8) & 0xFF;
                    int courseByte = rawValue & 0xFF;

                    position.setSpeed(speedKnots);
                    position.setAltitude(altByte * 10.0);
                    position.setCourse(courseByte * 2.0);

                    int gpsSats = satByte & 0x0F;
                    int glonassSats = (satByte >> 4) & 0x0F;
                    position.set(Position.KEY_SATELLITES, gpsSats + glonassSats);//исправлена калькуляция спутников
                    break;
                }

                
                case 6: {
                    int mode = rawValue & 0xFF;              // Byte[0] - mode
                    int b1   = (rawValue >> 8) & 0xFF;       // Byte[1]
                    int val  = (rawValue >> 16) & 0xFFFF;    // Bytes[2..3]

                    // ===== DISCRETE MODE (0x01) =====
                    if (mode == 0x01) {

                    int virtualMask = b1;   // virtual sensors
                    int ioMask = val;       // physical IN1..IN16

                    // virtual ignition
                    //Недокументированные особенности работы virtual ignition:
                    //1 - Зажигание по напряжению, если нет, то:
                    //2 - зажигание по Кану, если нет, то:
                    //3 - зажигание по состоянию входа с типом "Зажигание"
                    boolean virtualIgnition = (virtualMask & 0x01) != 0;
                    position.set("virtualIgnition", virtualIgnition);
                    //position.set(Position.KEY_IGNITION, virtualIgnition); optional | опционально, лучше использовать вычисляемый атрибут

                    // physical inputs IN1..IN16
                    for (int i = 0; i < 16; i++) {
                    position.set("in" + (i + 1), (ioMask & (1 << i)) != 0);
                    }

                    // optional: raw value for debug / Wialon compatibility
                    //position.set("pinRaw", String.format("0x%02X%04X", virtualMask, ioMask));
                }

                    // ===== OTHER MODES (impulses / freq / analog) =====
                    else {
                    int inputNumber = b1 + 1; // inputs are 1-based
                    position.set("pinMode" + inputNumber, mode);
                    position.set("pinValue" + inputNumber, val);
                    }
                    break;
                }
                case 8: {
                    int signalLevel = rawValue & 0xFF;
                    int mcc = (rawValue >> 8) & 0xFFFF;
                    int mnc = (rawValue >> 24) & 0xFF;
                
                    if (signalLevel >= 0 && signalLevel <= 31) {
                        position.set(Position.KEY_RSSI, signalLevel);
                    }
                    position.set("mcc", mcc);
                    position.set("mnc", mnc);
                    break;
                }
                case 9: {
                    long status = rawValue;
                    position.set(Position.KEY_POWER, BitUtil.from(status, 24) * 150 / 1000.0);
                    position.set(Position.KEY_STATUS, status);
                    break;
                }
                // ======================
                // CAN DATA
                // ======================
                case 52: {
                    double engineHours = rawValue / 100.0;
                    position.set("EngineHours", engineHours);
                    break;
                }
                case 53: {
                    double km = rawValue / 100.0;
                    position.set(Position.KEY_ODOMETER, km * 1000);
                    bytesRead += 4;
                    break;
                }
                case 55: {
                    position.set(Position.KEY_FUEL_LEVEL, rawValue / 10.0); //FUEL_LEVEL %
                    bytesRead += 4;
                    break;
                }
                case 56: {
                    position.set("CANFuel", rawValue / 10.0); //FUEL_LEVEL liters
                    bytesRead += 4;
                    break;
                }
                case 57: {
                    position.set(Position.KEY_RPM, rawValue);
                    bytesRead += 4;
                    break;
                }
                case 58: {
                    long value = rawValue;
                    double temp = value;
                    if (temp > 1000) temp = -(temp - 1000);
                    position.set(Position.KEY_ENGINE_TEMP, temp);
                    bytesRead += 4;
                    break;
                }
                case 59: {
                    position.set(Position.KEY_OBD_SPEED, rawValue);
                    bytesRead += 4;
                    break;
                }
                case 60: case 61: case 62: case 63: case 64: {
                    int index = tag - 59;
                    position.set(Position.KEY_AXLE_WEIGHT + index, rawValue);
                    bytesRead += 4;
                    break;
                }
                case 69: {
                    long value = rawValue;
                    int throttle = (int) (value & 0xFF);
                    int engineLoad = (int) ((value >> 8) & 0xFF);
                    position.set(Position.KEY_THROTTLE, throttle);
                    position.set(Position.KEY_ENGINE_LOAD, engineLoad);
                    bytesRead += 4;
                    break;
                }

                // ======================
                // LLS
                // ======================
                case 70: case 71: case 72: case 73: case 74:
                case 75: case 76: case 77: case 78: case 79: {
                    long value = rawValue;
                    int levelRaw = (int) (value & 0xFFFF);
                    int tempRaw = (int) ((value >> 16) & 0xFFFF);
                    int index = tag - 69;
                    position.set(Position.KEY_FUEL + index, levelRaw);
                    position.set(Position.PREFIX_TEMP + index, tempRaw);
                    bytesRead += 4;
                    break;
                }
                case 151: {
                    int hdopX100 = rawValue & 0xFFFF;
                    double hdop = hdopX100 / 100.0;
                    position.set(Position.KEY_HDOP, hdop);
                    break;
                }

                default: {
                    // для отладки
                    // System.out.println("Unknown tag: " + tag);
                    break;
                }
            }
        }

        int remaining = length - (buf.readerIndex() - startIndex);
        if (remaining > 0) buf.skipBytes(remaining);

        return position;
    }

    @Override
    protected Object decode(Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.readableBytes() == 0) return null;

        if (buf.getByte(buf.readerIndex()) == HEADER_START_SIGN) {
            if (buf.readableBytes() < 10) return null;
            buf.readByte();
            byte version = buf.readByte();
            String imei = String.valueOf(buf.readLongLE());
            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, imei);
            if (deviceSession != null) sendResponse(channel, version, 0);
            return null;
        }

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
        if (deviceSession == null) return null;

        List<Position> positions = new LinkedList<>();

        // Обрабатываем все пакеты подряд. Исправление ошибки IndexOutOfBoundsException, 
        // когда пакет содержит несколько вложенных пакетов (0x5B … 0x5D) подряд.
        while (buf.readableBytes() > 0 && buf.getByte(buf.readerIndex()) == PACKAGE_START) {
            buf.readByte(); // PACKAGE_START
            int packageIndex = buf.readUnsignedByte();

            while (buf.readableBytes() > 0) {
                if (buf.getByte(buf.readerIndex()) == PACKAGE_END) {
                    buf.readByte(); // PACKAGE_END
                    break;
                }

                if (buf.readableBytes() < 7) break;
                byte packetType = buf.readByte();
                int dataLength = buf.readUnsignedShortLE();
                Date time = new Date(buf.readUnsignedIntLE() * 1000L);

                if (buf.readableBytes() < dataLength + 1) break;
                // если данных меньше, чем указано, прекращаем чтение
                if (packetType == RECORD_DATA) {
                    Position position = decodePosition(deviceSession, buf, dataLength, time);
                    if (position.getLatitude() != 0 && position.getLongitude() != 0) {
                        positions.add(position);
                    }
                } else {
                    buf.skipBytes(dataLength);
                }

                if (buf.readableBytes() > 0) buf.readByte(); // checksum
            }

            sendResponse(channel, HEADER_VERSION_1, packageIndex);
        }

        return positions.isEmpty() ? null : positions;
    }
}
