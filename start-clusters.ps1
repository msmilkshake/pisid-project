$job1 = Start-Job -ScriptBlock { mongod.exe --config "C:\sensor-replica-set\server1\server1.conf" }
Start-Sleep -Seconds 3
Receive-Job -Job $job1

Start-Sleep -Seconds 1

$initiateOutput = & mongosh --port 27019 --eval "rs.initiate(); exit;" 2>&1
$initialized = ($initiateOutput -match ".*already initialized.*" -join " ").ToString() -notlike "*already initialized*"

if ($initialized) {
    Write-Host "Replica set is not initialized. Configuring..."
    & mongosh --port 27019 --eval "rs.add('localhost:25019'); exit;" --quiet
    & mongosh --port 27019 --eval "rs.add('localhost:23019'); exit;" --quiet
} else {
    Write-Host "Replica set is already configured."
}

$job2 = Start-Job -ScriptBlock { mongod.exe --config "C:\sensor-replica-set\server2\server2.conf" }
$job3 = Start-Job -ScriptBlock { mongod.exe --config "C:\sensor-replica-set\server3\server3.conf" }

Start-Sleep -Seconds 3

while (($job1.State, $job2.State, $job3.State) -contains 'Running')
{
    Receive-Job -Job $job1
    Receive-Job -Job $job2
    Receive-Job -Job $job3
    Start-Sleep -Seconds 5
}

Receive-Job -Job $job1
Receive-Job -Job $job2
Receive-Job -Job $job3