package configs

type MessageUnmarshaler interface {
	Unmarshal(data []byte, v interface{}) error
}
