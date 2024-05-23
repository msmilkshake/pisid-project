<?php
$db = "tailwag";
$dbhost = "dbhmsk.ddnsfree.com";
$dbport = "15427";

$username = $_POST["username"];
$password = $_POST["password"];
// $username = "servicesysadmin";
// $password = "myStr0ng..Pa\$\$w0Rd";
$conn = mysqli_connect($dbhost, $username, $password, $db, $dbport);

$sqlexperimid = <<<EOT
	SELECT IDExperiencia
	FROM experiencia
	WHERE IDEstado = 2
EOT;

$result = mysqli_query($conn, $sqlexperimid);
if ($result) {
	if (mysqli_num_rows($result) > 0) {
		$row = mysqli_fetch_assoc($result);
		$idexperim = $row['IDExperiencia'];
	}
}

$sql = <<<EOT
	SELECT sala, ratos
	FROM salas_ratos
	where experiencia = $idexperim
	ORDER BY sala
EOT;

$result = mysqli_query($conn, $sql);
$response["readings"] = array();
if ($result) {
	if (mysqli_num_rows($result) > 0) {
		while ($r = mysqli_fetch_assoc($result)) {
			$ad = array();
			// Alterar nome dos campos se necessario
			$ad["Room"] = $r['sala'];
			$ad["TotalMouse"] = $r['ratos'];
			array_push($response["readings"], $ad);
		}
	}
}

mysqli_close($conn);
header('Content-Type: application/json');
// tell browser that its a json data
echo json_encode($response);
//converting array to JSON string
