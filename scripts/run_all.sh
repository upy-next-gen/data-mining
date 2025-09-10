#!/bin/bash

# This script runs the ENSU data processing for all relevant files.

set -e # Exit immediately if a command exits with a non-zero status.

FILES_TO_PROCESS=(
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_01_2015/conjunto_de_datos_cb_ensu_01_2015/conjunto_de_datos/conjunto_de_datos_cb_ensu_01_2015.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_02_2015/conjunto_de_datos_cb_ensu_02_2015/conjunto_de_datos/conjunto_de_datos_cb_ensu_02_2015.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_03_2015/conjunto_de_datos_cb_ensu_03_2015/conjunto_de_datos/conjunto_de_datos_cb_ensu_03_2015.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_04_2015/conjunto_de_datos_cb_ensu_04_2015/conjunto_de_datos/conjunto_de_datos_cb_ensu_04_2015.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_01_2016_csv/conjunto_de_datos_cb_ensu_01_2016/conjunto_de_datos/conjunto_de_datos_cb_ensu_01_2016.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_02_2016_csv/conjunto_de_datos_cb_ensu_02_2016/conjunto_de_datos/conjunto_de_datos_cb_ensu_02_2016.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_03_2016_csv/conjunto_de_datos_cb_ensu_03_2016/conjunto_de_datos/conjunto_de_datos_cb_ensu_03_2016.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_04_2016_csv/conjunto_de_datos_cb_ensu_04_2016/conjunto_de_datos/conjunto_de_datos_cb_ensu_04_2016.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_01_2017_csv/conjunto_de_datos_cb_ensu_01_2017/conjunto_de_datos/conjunto_de_datos_cb_ensu_01_2017.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_02_2017_csv/conjunto_de_datos_cb_ensu_02_2017/conjunto_de_datos/conjunto_de_datos_cb_ensu_02_2017.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_03_2017_csv/conjunto_de_datos_cb_ensu_03_2017/conjunto_de_datos/conjunto_de_datos_cb_ensu_03_2017.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_04_2017_csv/conjunto_de_datos_cb_ensu_04_2017/conjunto_de_datos/conjunto_de_datos_cb_ensu_04_2017.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2018_1t_csv/ensu_cb_0118/conjunto_de_datos/ensu_cb_0118.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2018_2t_csv/ensu_cb_0218/conjunto_de_datos/ ensu_cb_0218.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2018_3t_csv/conjunto_de_datos_cb_ensu_03_2018/conjunto_de_datos/conjunto_de_datos_cb_ensu_03_2018.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2018_4t_csv/conjunto_de_datos_cb_ensu_04_2018/conjunto_de_datos/conjunto_de_datos_cb_ENSU_04_2018.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ENSU_2019_1t_csv/conjunto_de_datos_cb_ensu_03_2019/conjunto_de_datos/conjunto_de_datos_cb_ensu_03_2019.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2019_2t_csv/conjunto_de_datos_cb_ENSU_2019_2t/conjunto_de_datos/conjunto_de_datos_cb_ENSU_2019_2t.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2019_4t_csv/conjunto_de_datos_CB_ENSU_04_2019/conjunto_de_datos/conjunto_de_datos_CB_ENSU_04_2019.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2020_1t_csv/conjunto_de_datos_CB_ENSU_01_2020/conjunto_de_datos/conjunto_de_datos_CB_ENSU_01_2020.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2020_3t_csv/conjunto_de_datos_CB_SEC1_2_3_ENSU_09_2020/conjunto_de_datos/conjunto_de_datos_CB_SEC1_2_3_ENSU_09_2020.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2020_3t_csv/conjunto_de_datos_CB_SEC4_ENSU_09_2020/conjunto_de_datos/conjunto_de_datos_CB_SEC4_ENSU_09_2020.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2020_4t_csv/conjunto_de_datos_CB_ENSU_12_2020/conjunto_de_datos/conjunto_de_datos_CB_ENSU_12_2020.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2021_1t_csv/conjunto_de_datos_CB_ENSU_03_2021/conjunto_de_datos/conjunto_de_datos_CB_ENSU_03_2021.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2021_2t_csv/conjunto_de_datos_CB_ENSU_06_2021/conjunto_de_datos/conjunto_de_datos_CB_ENSU_06_2021.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2021_3t_csv/conjunto_de_datos_CB_ENSU_09_2021/conjunto_de_datos/conjunto_de_datos_CB_ENSU_09_2021.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2021_4t_csv/conjunto_de_datos_CB_ENSU_12_2021/conjunto_de_datos/conjunto_de_datos_CB_ENSU_12_2021.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2022_1t_csv/conjunto_de_datos_ensu_cb_0322/conjunto_de_datos/conjunto_de_datos_ensu_cb_0322.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2022_2t_csv/conjunto_de_datos_ensu_cb_0622/conjunto_de_datos/conjunto_de_datos_ensu_cb_0622.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2022_3t_csv/conjunto_de_datos_ensu_cb_0922/conjunto_de_datos/conjunto_de_datos_ensu_cb_0922.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2022_4t_csv/conjunto_de_datos_ensu_cb_1222/conjunto_de_datos/conjunto_de_datos_ensu_cb_1222.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2023_1t_csv/conjunto_de_datos_CB_ENSU_03_2023/conjunto_de_datos/conjunto_de_datos_CB_ENSU_03_2023.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2023_2t_csv/conjunto_de_datos_ensu_cb_0623/conjunto_de_datos/conjunto_de_datos_ensu_cb_0623.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2023_3t_csv/conjunto_de_datos_ensu_cb_0923/conjunto_de_datos/conjunto_de_datos_ensu_cb_0923.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2023_4t_csv/conjunto_de_datos_ensu_cb_1223/conjunto_de_datos/conjunto_de_datos_ensu_cb_1223.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2024_1t_csv/conjunto_de_datos_ensu_cb_0324/conjunto_de_datos/conjunto_de_datos_ensu_cb_0324.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2024_2t_csv/conjunto_de_datos_ensu_cb_0624/conjunto_de_datos/conjunto_de_datos_ensu_cb_0624.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2024_3t_csv/conjunto_de_datos_ensu_cb_0924/conjunto_de_datos/conjunto_de_datos_ensu_cb_0924.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2024_4t_csv/conjunto_de_datos_ensu_cb_1224/conjunto_de_datos/conjunto_de_datos_ensu_cb_1224.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2025_1t_csv/conjunto_de_datos_ensu_cb_0325/conjunto_de_datos/conjunto_de_datos_ensu_cb_0325.csv"
    "/mnt/c/Users/isaac/Data_Mining/data-mining/data/conjunto_de_datos_ensu_2025_2t_csv/conjunto_de_datos_ensu_cb_0625/conjunto_de_datos/conjunto_de_datos_ensu_cb_0625.csv"
)

for file in "${FILES_TO_PROCESS[@]}"
do
    echo "----------------------------------------------------"
    echo "Executing processing for: $file"
    uv run python process_ensu_data.py "$file"
    echo "----------------------------------------------------
"
done

echo "All files processed."