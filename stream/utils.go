package stream

func isTrue(v interface{}) bool {
	if b, ok := v.(bool); ok && b {
		return true
	}
	return false
}
