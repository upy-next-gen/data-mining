# Script para configurar el entorno virtual
# Autor: Cascade AI
# Fecha: 2025-09-10

# Verificar si PowerShell está en modo administrador
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "Nota: Algunos comandos pueden requerir privilegios de administrador." -ForegroundColor Yellow
}

# Crear directorios si no existen
$directories = @(
    ".\data",
    ".\data\yucatan-inseguridad",
    ".\data\yucatan-inseguridad\insumos",
    ".\data\yucatan-inseguridad\insumos\raw",
    ".\data\yucatan-inseguridad\insumos\diccionarios",
    ".\data\yucatan-inseguridad\procesados",
    ".\data\yucatan-inseguridad\reportes",
    ".\data\yucatan-inseguridad\logs"
)

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        Write-Host "Creando directorio: $dir" -ForegroundColor Green
        New-Item -Path $dir -ItemType Directory -Force | Out-Null
    }
}

# Verificar si Python está instalado
try {
    $pythonVersion = python --version
    Write-Host "Python detectado: $pythonVersion" -ForegroundColor Green
}
catch {
    Write-Host "Error: No se pudo encontrar Python. Por favor instale Python 3.8 o superior." -ForegroundColor Red
    exit 1
}

# Crear entorno virtual si no existe
if (-not (Test-Path ".\.venv")) {
    Write-Host "Creando entorno virtual..." -ForegroundColor Cyan
    python -m venv .venv
    
    if (-not $?) {
        Write-Host "Error: No se pudo crear el entorno virtual." -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "El entorno virtual ya existe." -ForegroundColor Green
}

# Activar el entorno virtual
Write-Host "Activando entorno virtual..." -ForegroundColor Cyan
try {
    & .\.venv\Scripts\Activate.ps1
}
catch {
    Write-Host "Error activando el entorno virtual. Intente ejecutar manualmente: .\.venv\Scripts\Activate.ps1" -ForegroundColor Red
    exit 1
}

# Instalar dependencias
Write-Host "Instalando dependencias..." -ForegroundColor Cyan
pip install -r requirements.txt

if (-not $?) {
    Write-Host "Error al instalar las dependencias." -ForegroundColor Red
    exit 1
}

Write-Host "Entorno listo." -ForegroundColor Green
