go build

linux:
$Env:GOOS = "linux"; $Env:GOARCH = "amd64"; go build