package ifs

func ConvertOpCodeToString(opCode uint8) string {

	switch opCode {
	case AttrRequest:
		return "Attr Request"
	case ReadDirRequest:
		return "ReadDir Request"
	case FetchFileRequest:
		return "FetchFile Request"
	case ReadFileRequest:
		return "ReadFile Request"
	case WriteFileRequest:
		return "WriteFile Request"
	case TruncateRequest:
		return "Truncate Request"
	case CreateRequest:
		return "Create Request"
	case RemoveRequest:
		return "Remove Request"

	case StatResponse:
		return "Stat Response"
	case StatsResponse:
		return "Stats Response"
	case FileDataResponse:
		return "FileData Response"
	case WriteResponse:
		return "Write Response"
	case ErrorResponse:
		return "Error Response"
	}

	return "Unknown Op"
}
