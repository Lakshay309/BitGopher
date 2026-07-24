package discovery

import (
	"crypto/rand"
	"errors"

	"golang.org/x/crypto/argon2"
)

func DeriveKey(password string) [32]byte {
	key := argon2.IDKey(
		[]byte(password),
		salt,
		1,
		64*1024,
		4,
		32,
	)
	var out [32]byte
	copy(out[:], key)
	return out
}

func (u *UdpServer) decryptData(buffer []byte) ([]byte, error) {
	if len(buffer) < u.gcm.NonceSize() {
		return nil, errors.New("packet too short")
	}

	nonceSize := u.gcm.NonceSize()
	nonce := buffer[:nonceSize]
	cipherText := buffer[nonceSize:]

	plainText, err := u.gcm.Open(
		nil,
		nonce,
		cipherText,
		nil,
	)
	if err != nil {
		// authentication failed or wrong password/key
		return nil, err
	}
	return plainText, nil
}

func (u *UdpServer) encryptData(data []byte) ([]byte, error) {
	nonce := make([]byte, u.gcm.NonceSize())

	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	packet := make([]byte, 0, len(nonce)+len(data)+u.gcm.Overhead())

	packet = append(packet, nonce...)

	packet = u.gcm.Seal(packet, nonce, data, nil)

	return packet, nil
}
