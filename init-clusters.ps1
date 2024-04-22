$job1 = Start-Job -ScriptBlock { mongod.exe --config "C:\sensor-replica-set\server1\server1.conf" }
Start-Sleep -Seconds 3
Receive-Job -Job $job1

Start-Sleep -Seconds 1

Write-Host "Configuring ReplicaSet:"
$initiateOutput = & mongosh --port 27019 --eval "rs.initiate()" 2>&1
$initialized = ($initiateOutput -match ".*already initialized.*" -join " ").ToString() -notlike "*already initialized*"

if ($initialized) {
    Write-Host "Replica set is not initialized. Configuring..."

    Write-Host "Creating user:"
    $testOutput = & mongosh --port 27019 --eval "use admin" --eval "db.createUser({user: 'rootuser', pwd: 'rootpassword',roles:[{role:'root', db:'admin'}]})" 2>&1
    Write-Host $testOutput

    Write-Host "Adding clusters..."
    & mongosh --port 27019 --eval "rs.add('localhost:25019'); exit;" --quiet
    & mongosh --port 27019 --eval "rs.add('localhost:23019'); exit;" --quiet
} else {
    Write-Host "Replica set is already configured."
}
Start-Sleep -Seconds 3

Write-Host "Creating ReplicaSet key..."
$key = "lDFHikshdEfgauGHkBdas3Jr67rb7cvxtJya"
Set-Content -Path "c:\sensor-replica-set\mongodb-keyfile" -Value $key

Stop-Job -Job $job1
Remove-Job -Job $job1

Write-Host "Replica set configured."