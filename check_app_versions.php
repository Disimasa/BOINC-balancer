<?php
// Скрипт для проверки версий приложений в базе данных

chdir("html/inc");
require_once("boinc_db.inc");
require_once("util_basic.inc");
chdir("../..");

$apps = BoincApp::enum("");
$platforms = BoincPlatform::enum("");

echo "Приложения в базе данных:\n";
foreach ($apps as $app) {
    echo "  - {$app->name} (id: {$app->id})\n";
}

echo "\nПлатформы в базе данных:\n";
foreach ($platforms as $platform) {
    echo "  - {$platform->name} (id: {$platform->id})\n";
}

echo "\nПроверяю версии приложений в базе данных:\n";
$app_versions = BoincAppVersion::enum("");
foreach ($app_versions as $av) {
    $app = BoincApp::lookup_id($av->appid);
    $platform = BoincPlatform::lookup_id($av->platformid);
    echo "  - {$app->name} {$av->version_num} ({$platform->name}) - id: {$av->id}\n";
}

echo "\nПроверяю директории приложений:\n";
$app_dirs = glob("apps/*/1.0/*/version.xml");
foreach ($app_dirs as $dir) {
    $parts = explode("/", $dir);
    $app_name = $parts[1];
    $version = $parts[2];
    $platform = $parts[3];
    echo "  - Found: $app_name $version $platform\n";
}

?>


