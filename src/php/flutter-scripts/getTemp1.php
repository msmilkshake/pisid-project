<?php
$db = "tailwag";
$dbhost = "dbhmsk.ddnsfree.com";
$dbport = "15427";

$username = $_POST["username"];
$password = $_POST["password"];
// $username = "servicesysadmin";
// $password = "myStr0ng..Pa\$\$w0Rd";
$conn = mysqli_connect($dbhost, $username, $password, $db, $dbport);

$sql = <<<EOT
	SELECT Hora, Leitura
	from medicoestemperatura
	where Sensor = 1
		AND IsError = 0
		AND IsOutlier = 0
		AND Hora >= now() - interval 10 minute
	ORDER BY Hora DESC
EOT;

$result = mysqli_query($conn, $sql);
$response["readings"] = array();
if ($result) {
	if (mysqli_num_rows($result) > 0) {
		try {
			while ($r = mysqli_fetch_assoc($result)) {
				$ad = array();
				$ad["Hora"] = $r['Hora'];
				$ad["Leitura"] = $r['Leitura'];
				array_push($response["readings"], $ad);
			}
		} catch (Exception $e) {
			echo ($e);
		}
	}
}

mysqli_close($conn);
header('Content-Type: application/json');
// tell browser that its a json data
echo json_encode($response);
//converting array to JSON string
