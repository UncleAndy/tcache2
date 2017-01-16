package sletat

func IsSkipPacket(packet *SletatPacket) bool {
	if !IsDepartCityActive(packet.DptCityId) {
		return true
	}

	return false
}
