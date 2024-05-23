<?php
$db = "tailwag"; //database name
$username = $_POST["username"];
$password = $_POST["password"];
// $username = "servicesysadmin";
// $password = "myStr0ng..Pa\$\$w0Rd";
$dbhost = "dbhmsk.ddnsfree.com";
$dbport = "15427";

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
	SELECT Alerta.Hora, Alerta.Mensagem, Alerta.TipoAlerta, TipoAlerta.Designacao
	FROM Alerta
	JOIN TipoAlerta ON Alerta.TipoAlerta = TipoAlerta.IDTipoAlerta
	WHERE Alerta.Hora >= now() - interval 15 minute
		AND Alerta.IDExperiencia = $idexperim
	ORDER BY Alerta.Hora DESC
EOT;

$response["alerts"] = array();
$result = mysqli_query($conn, $sql);
if ($result) {
	if (mysqli_num_rows($result) > 0) {
		while ($r = mysqli_fetch_assoc($result)) {
			try {
				$ad = array();
				$ad["Hora"] = $r['Hora'];
				$ad["Mensagem"] = $r['Mensagem'];
				$ad["TipoAlerta"] = $r['Designacao'];
				array_push($response["alerts"], $ad);
			} catch (Exception $e) {
				echo ($e);
			}
		}
	}
}

header('Content-Type: application/json');
// tell browser that its a json data
echo json_encode($response);
//converting array to JSON string
