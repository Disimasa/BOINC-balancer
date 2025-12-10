<?php
// Скрипт для принудительного добавления версий приложений в базу данных

chdir("html/inc");
require_once("boinc_db.inc");
require_once("util_basic.inc");
chdir("../..");

$apps_to_process = ["fast_task", "medium_task", "long_task", "random_task"];
$platform_mapping = [
    "x86_64-pc-linux-gnu__vbox64_mt" => "x86_64-pc-linux-gnu",
    "windows_x86_64__vbox64_mt" => "windows_x86_64",
    "x86_64-apple-darwin__vbox64_mt" => "x86_64-apple-darwin"
];

$all_apps = BoincApp::enum("");
$apps_map = [];
foreach ($all_apps as $a) {
    $apps_map[$a->name] = $a;
}

foreach ($apps_to_process as $app_name) {
    if (!isset($apps_map[$app_name])) {
        echo "ERROR: App $app_name not found in database\n";
        continue;
    }
    $app = $apps_map[$app_name];
    
    echo "Processing app: $app_name (id: {$app->id})\n";
    
    $version_dir = "apps/$app_name/1.0";
    if (!is_dir($version_dir)) {
        echo "  ERROR: Version directory not found: $version_dir\n";
        continue;
    }
    
    // Обрабатываем каждую платформу
    foreach ($platform_mapping as $platform_dir => $platform_name) {
        $full_path = "$version_dir/$platform_dir";
        if (!is_dir($full_path)) {
            echo "  WARNING: Platform directory not found: $full_path\n";
            continue;
        }
        
        $all_platforms = BoincPlatform::enum("");
        $platform = null;
        foreach ($all_platforms as $p) {
            if ($p->name == $platform_name) {
                $platform = $p;
                break;
            }
        }
        if (!$platform) {
            echo "  ERROR: Platform $platform_name not found in database\n";
            continue;
        }
        
        // Проверяем, существует ли уже версия
        $existing = BoincAppVersion::lookup("appid={$app->id} and platformid={$platform->id} and version_num=100");
        if ($existing) {
            echo "  Already exists: $app_name 1.0 ($platform_name) - id: {$existing->id}\n";
            continue;
        }
        
        // Создаем новую версию через SQL
        $db = BoincDb::get();
        $create_time = time();
        $version_num = 100; // 1.0 = 100
        $plan_class = "vbox64_mt";
        
        $query = "INSERT INTO app_version (appid, version_num, platformid, plan_class, create_time, deprecated, min_core_version, max_core_version) " .
                 "VALUES ({$app->id}, $version_num, {$platform->id}, '$plan_class', $create_time, 0, 0, 0)";
        
        $result = $db->do_query($query);
        if ($result) {
            $id = $db->insert_id();
            echo "  Added: $app_name 1.0 ($platform_name) - id: $id\n";
        } else {
            echo "  ERROR: Failed to add version for $app_name 1.0 ($platform_name): " . $db->error() . "\n";
        }
    }
}

echo "\nDone!\n";

?>

