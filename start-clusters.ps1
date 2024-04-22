# --auth --keyFile "c:\sensor-replica-set\mongodb-keyfile"
$job1 = Start-Job -ScriptBlock { mongod.exe --config "C:\sensor-replica-set\server1\server1.conf" }
Start-Sleep -Seconds 3
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