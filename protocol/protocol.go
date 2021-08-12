/*
Copyright 2017 Crunchy Data Solutions, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package protocol

import (
	"bytes"
        "strings"
	"encoding/binary"
     	"github.com/crunchydata/crunchy-proxy/util/log"
)

/* PostgreSQL Protocol Version/Code constants */
const (
	ProtocolVersion int32 = 196608
	SSLRequestCode  int32 = 80877103

	/* SSL Responses */
	SSLAllowed    byte = 'S'
	SSLNotAllowed byte = 'N'
)

/* PostgreSQL Message Type constants. */
const (
	AuthenticationMessageType  byte = 'R'
	ErrorMessageType           byte = 'E'
	EmptyQueryMessageType      byte = 'I'
	DescribeMessageType        byte = 'D'
	RowDescriptionMessageType  byte = 'T'
	DataRowMessageType         byte = 'D'
	QueryMessageType           byte = 'Q'
	CommandCompleteMessageType byte = 'C'
	TerminateMessageType       byte = 'X'
	NoticeMessageType          byte = 'N'
	PasswordMessageType        byte = 'p'
	ReadyForQueryMessageType   byte = 'Z'
)

/* PostgreSQL Authentication Method constants. */
const (
	AuthenticationOk          int32 = 0
	AuthenticationKerberosV5  int32 = 2
	AuthenticationClearText   int32 = 3
	AuthenticationMD5         int32 = 5
	AuthenticationSCM         int32 = 6
	AuthenticationGSS         int32 = 7
	AuthenticationGSSContinue int32 = 8
	AuthenticationSSPI        int32 = 9
)

func GetVersion(message []byte) int32 {
	var code int32

	reader := bytes.NewReader(message[4:8])
	binary.Read(reader, binary.BigEndian, &code)

	return code
}

/*
 * Get the message type the provided message.
 *
 * message - the message
 */
func GetMessageType(message []byte) byte {
	return message[0]
}

/*
 * Get the message length of the provided message.
 *
 * message - the message
 */
func GetMessageLength(message []byte) int32 {
	var messageLength int32

	reader := bytes.NewReader(message[1:5])
	binary.Read(reader, binary.BigEndian, &messageLength)

	return messageLength
}

func clen(n []byte) int {
    for i := 0; i < len(n); i++ {
        if n[i] == 0 {
            return i
        }
    }
    return len(n)
}

/*
 * 
 *
 * 
 */
func GetColumnIndex(message []byte, columnName string) int16 {
	var messageLength int32
        var numColumns int16

        messageType := message[0]
        /* If it's not a column message, ignore it. */
        if messageType != 84 {
           log.Info("Message type was not 84")
           return 0
        }

	reader := bytes.NewReader(message[1:5])
	binary.Read(reader, binary.BigEndian, &messageLength)

	reader = bytes.NewReader(message[5:7])
	binary.Read(reader, binary.BigEndian, &numColumns)

        start := int32(7)
        i := start
        for j := int16(0); j < numColumns; j += 1 {
            nullIndex := clen(message[i:])
            end := i + int32(nullIndex)
            tmp := string(message[i:end])
            log.Infof("Found tmp: %s", tmp)
            log.Infof("Found tmp: %x", tmp)
            log.Infof("col: %x", columnName)
            if strings.Compare(columnName, tmp) == 0 {
               log.Info("WIN")
               return j
            }
            log.Infof("j: %d i: %d end: %d colName: %s", j, i, end, columnName)
            i = int32(end + 19) /* that's the number of trailing bytes after the null term string */
        }

	return -1
}

/*
 * 
 *
 * 
 */
func GetDataByColumnIndex(message []byte, columnIndex int16) []byte {
	var messageLength int32
        var numColumns int16
	var data []byte

        messageType := message[0]
        /* If it's not a data message, ignore it. */
        if messageType != 68 {
           log.Info("Message type was not 68")
           return message
        }
	if columnIndex == -1 {
		log.Info("Column index was -1")
		return message
	}
	reader := bytes.NewReader(message[1:5])
	binary.Read(reader, binary.BigEndian, &messageLength)

	reader = bytes.NewReader(message[5:7])
	binary.Read(reader, binary.BigEndian, &numColumns)

        start := int32(7)
        i := start
        var fieldSize int32
        for j := int16(0); j < numColumns; j += 1 {
      	    reader = bytes.NewReader(message[i:i+4])
            binary.Read(reader, binary.BigEndian, &fieldSize)
            log.Infof("j %d Found field size: %d", j, fieldSize)
            i += 4
            if j == columnIndex {
		    log.Infof("j %d For columnIndex %d we have %x", j, columnIndex, message[i:i+fieldSize])
		    data = append(data, message[i:i+fieldSize]...)
            }
            log.Infof("For j %d we have %x", j, message[i:i+fieldSize])
            i += fieldSize
        }

	return data
}

/*
 * 
 *
 * 
 */
func NewDataMessageInsertByColumnIndex(message []byte, columnIndex int16, newData []byte) []byte {
	var messageLength int32
        var numColumns int16
	var newPayload []byte
	var newLength int32

	log.Infof("O.G. message: %s", message)
	log.Infof("O.G. message in hex: %x", message)
	
        messageType := message[0]
        /* If it's not a data message, ignore it. */
        if messageType != 68 {
           log.Info("Message type was not 68")
           return message
        }
	if columnIndex == -1 {
		log.Info("Column index was -1")
		return message
	}

	reader := bytes.NewReader(message[1:5])
	binary.Read(reader, binary.BigEndian, &messageLength)

	reader = bytes.NewReader(message[5:7])
	binary.Read(reader, binary.BigEndian, &numColumns)

	newLength = 6 /* int32 size field, plus int16 num columns, ignore type byte in length */
	
        start := int32(7)
        i := start
        var fieldSize int32
        for j := int16(0); j < numColumns; j += 1 {
      	    reader = bytes.NewReader(message[i:i+4])
            binary.Read(reader, binary.BigEndian, &fieldSize)
            log.Infof("j %d Found field size: %d", j, fieldSize)
            if j == columnIndex {
		    log.Infof("j %d For columnIndex %d we have %x", j, columnIndex, message[i+4:i+4+fieldSize])
		    newSize := make([]byte, 4)
		    binary.BigEndian.PutUint32(newSize, uint32(len(newData)))
		    newLength += 4 + int32(len(newData))
		    newPayload = append(newPayload, newSize...)
		    newPayload = append(newPayload, newData...)
            } else {
		    /* if it's not the column data we're replacing, then just append the
		       field size and data of size fieldSize.
		    */
		    newLength += 4 + fieldSize
		    /* note this is copying the size plus the data */
		    newPayload = append(newPayload, message[i:i+4+fieldSize]...)
	    }
            log.Infof("For j %d we have %x", j, message[i+4:i+4+fieldSize])
            i += 4+fieldSize
        }

	var newMessage []byte
	newMessage = append(newMessage, message[0])
	newLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(newLengthBytes, uint32(newLength))
	newMessage = append(newMessage, newLengthBytes...)

	/* column count stays the same */
	newMessage = append(newMessage, message[5:7]...)

	newMessage = append(newMessage, newPayload...)
	
	return newMessage
}


/* IsAuthenticationOk
 *
 * Check an Authentication Message to determine if it is an AuthenticationOK
 * message.
 */
func IsAuthenticationOk(message []byte) bool {
	/*
	 * If the message type is not an Authentication message, then short circuit
	 * and return false.
	 */
	if GetMessageType(message) != AuthenticationMessageType {
		return false
	}

	var messageValue int32

	// Get the message length.
	messageLength := GetMessageLength(message)

	// Get the message value.
	reader := bytes.NewReader(message[5:9])
	binary.Read(reader, binary.BigEndian, &messageValue)

	return (messageLength == 8 && messageValue == AuthenticationOk)
}

func GetTerminateMessage() []byte {
	var buffer []byte
	buffer = append(buffer, 'X')

	//make msg len 1 for now
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4))
	buffer = append(buffer, x...)
	return buffer
}
