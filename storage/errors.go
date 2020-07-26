package storage

import "errors"

var (
	AlreadyDeletedError = errors.New("Already deleted.")
	NoSpaceError        = errors.New("The page does not have enough space.")
	NoSuchSlotError     = errors.New("The page does not have the slot.")
	DuplicateKeyError   = errors.New("The key is duplicate.")
	NoKeyError          = errors.New("The key does not exist.")
)
