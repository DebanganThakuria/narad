package users

import "golang.org/x/crypto/bcrypt"

// bcryptHash hashes a plaintext password for storage.
func bcryptHash(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// bcryptCompare reports whether password matches the stored hash (nil)
// or not (non-nil).
func bcryptCompare(hash []byte, password string) error {
	return bcrypt.CompareHashAndPassword(hash, []byte(password))
}
