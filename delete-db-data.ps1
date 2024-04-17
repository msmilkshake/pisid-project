Get-ChildItem "C:\sensor-replica-set\server*" -Directory | ForEach-Object {
    Remove-Item "$($_.FullName)\data\*" -Recurse
    Remove-Item "$($_.FullName)\logs\*" -Recurse
}