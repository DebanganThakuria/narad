package remotecred

// Pinned outputs of deriveMaterial(vectorSecret, vectorSalt): HKDF-SHA-512
// with the three purpose labels. They were checked against an
// independent RFC 5869 implementation (Python's hmac and hashlib) when
// pinned, and TestHKDFMatchesTheStandardLibrary checks them against
// crypto/hkdf on every run.
const (
	pinnedKey = "95b25861b7842509aad457c00ffcb03c1b8f082b3caa8ff322336b8a90b1006d"
	pinnedKV  = "2ed7760e8a2c6678"
	pinnedFP  = "c4f6d81ded58a7dd825e51a3e4c032c5f76bab4afeab225f815707f0f6ac571819881aa2a57afb5000def29576ed757f8c846b4f74b4b7f0f3b9380bca16bb9c"
)
