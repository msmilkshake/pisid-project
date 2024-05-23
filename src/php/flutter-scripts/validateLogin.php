<?php
$db = "tailwag";
$dbhost = "dbhmsk.ddnsfree.com";
$dbport = "15427";

$return["message"] = "";
$return["success"] = false;
$username = $_POST["username"];
$password = $_POST["password"];
// $username = "servicesysadmin";
// $password = "myStr0ng..Pa\$\$w0Rd";

try {
	$conn = mysqli_connect($dbhost, $username, $password, $db, $dbport);
	mysqli_close($conn);
	header('Content-Type: application/json');
	$return["success"] = true;
	echo json_encode($return);
} catch (Exception $e) {
	$return["message"] = "The login failed. Check if the user exists in the database. username:" . $username;
	header('Content-Type: application/json');
	echo json_encode($return);
}
