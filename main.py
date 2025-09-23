import polars as pl
import requests
import yfinance as yf
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import time
from pathlib import Path
import sys
from utils.logger import get_logger

# 📁 Configuración de rutas y logger
carpeta_db = Path('db')
ruta_data = carpeta_db / 'db.parquet'
logger = get_logger("actualiza_db")


# 🧩 Función de relleno final
def paso_final_actualizar(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Aplicando estrategia de forward fill")
    clave = ['date']
    df = (
        df
        .sort('date')
        .unique(subset=clave, keep='last')
    )
    return df.fill_null(strategy='forward')


# 🪙 Obtener data de Bitcoin
def retorna_data_bitcoin() -> dict:
    try:
        today = datetime.now(ZoneInfo("America/Caracas")).date()
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': 0,
            'interval': 'daily'
        }
        response = requests.get(url, params=params)
        data = response.json()
        diccionario = {
            'date': today,
            'price': data['prices'][0][1],
            'total_volume': data['total_volumes'][0][1],
            'market_cap': data['market_caps'][0][1]
        }
        logger.info("Data Bitcoin obtenida correctamente")
        return diccionario
    except Exception as e:
        logger.error(f"Error al obtener data de Bitcoin: {e}")
        raise


# 📊 Obtener data de instrumentos tradicionales
def retorna_data_instrumentos() -> dict:
    try:
        today = datetime.now(ZoneInfo("America/Caracas")).date()
        if today.weekday() in [5, 6]:
            logger.warning("Fin de semana detectado: retornando valores nulos")
            return {
                'price_gold': None,
                'stock_index_dowjones': None,
                'stock_index_sp500': None,
                'rate_US10Y': None,
                'stock_index_ni225': None
            }

        start = today.strftime('%Y-%m-%d')
        end = (today + timedelta(days=1)).strftime('%Y-%m-%d')
        tickers = {
            'price_gold': 'GC=F',
            'stock_index_dowjones': '^DJI',
            'stock_index_sp500': '^GSPC',
            'rate_US10Y': '^TNX',
            'stock_index_ni225': '^N225'
        }

        diccionario = {}
        for instrumento, simbolo in tickers.items():
            yahoo_response = yf.download(simbolo, start=start, end=end, interval='1d', auto_adjust=True)
            valor = yahoo_response.iloc[0, 0] if not yahoo_response.empty else None
            diccionario[instrumento] = valor
            logger.info(f"{instrumento} obtenido: {valor}")
            time.sleep(15)

        return diccionario
    except Exception as e:
        logger.error(f"Error al obtener data de instrumentos: {e}")
        raise


# 📦 Cargar data existente
def cargar_data_existente(ruta: str) -> pl.DataFrame:
    try:
        df = pl.read_parquet(ruta)
        logger.info(f"Data existente cargada desde {ruta}")
        return df
    except Exception as e:
        logger.error(f"Error al cargar data existente: {e}")
        raise


# 🧪 Obtener data de hoy
def obtener_data_hoy() -> pl.DataFrame:
    try:
        data_bitcoin = retorna_data_bitcoin()
        data_instrumentos = retorna_data_instrumentos()
        data_combinado = data_bitcoin | data_instrumentos
        logger.info(f"Data combinada: {data_combinado}")
        return pl.DataFrame(data_combinado)
    except Exception as e:
        logger.error(f"Error al obtener data de hoy: {e}")
        raise


# 🔗 Concatenar data
def concatenar(df_existente: pl.DataFrame, df_hoy: pl.DataFrame) -> pl.DataFrame:
    try:
        df_concat = pl.concat([df_existente, df_hoy], how='vertical')
        logger.info("Data concatenada correctamente")
        return df_concat
    except Exception as e:
        logger.error(f"Error al concatenar data: {e}")
        raise


# 🔄 Actualizar data
def actualizar(df: pl.DataFrame) -> pl.DataFrame:
    try:
        df_actualizado = paso_final_actualizar(df)
        logger.info("Data actualizada correctamente")
        return df_actualizado
    except Exception as e:
        logger.error(f"Error al actualizar data: {e}")
        raise


# 💾 Persistir data
def persistir(df: pl.DataFrame, ruta: str) -> None:
    try:
        df.write_parquet(ruta)
        logger.info(f"Data persistida exitosamente en {ruta}")
    except Exception as e:
        logger.error(f"Error al persistir data: {e}")
        raise


# 🚀 Pipeline principal
def pipeline_principal():
    for intento in range(1, 4):
        try:
            logger.info(f"Inicio del pipeline de actualización INTENTO --> {intento}")
            (
                cargar_data_existente(ruta_data)
                .pipe(lambda df: concatenar(df, obtener_data_hoy()))
                .pipe(actualizar)
                .pipe(lambda df: persistir(df, ruta_data))
            )
            logger.info("Pipeline completado exitosamente")
            return
        except Exception as e:
            logger.critical(f"Pipeline falló en intento {intento}: {e}")
            time.sleep(60)
    logger.warning("Pipeline falló tras 3 intentos consecutivos")
    return


def limpia_logs_viejos():
    carpeta_logs = Path("logs")
    hoy = datetime.now().date()
    dias_limite = 15
    eliminados = 0

    if not carpeta_logs.exists():
        logger.warning("Carpeta de logs no existe. No se realizó limpieza.")
        return

    for archivo in carpeta_logs.iterdir():
        if archivo.is_file() and archivo.name.startswith("log_actualiza_db_") and archivo.name.endswith(".log"):
            try:
                partes = archivo.name.split("_")
                fecha_str = partes[3].replace(".log", "")
                fecha_log = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                diferencia = (hoy - fecha_log).days

                if diferencia > dias_limite:
                    archivo.unlink()
                    eliminados += 1
                    logger.info(f"Log eliminado: {archivo.name} (antigüedad: {diferencia} días)")
            except Exception as e:
                logger.error(f"Error al procesar log {archivo.name}: {e}")

    if eliminados == 0:
        logger.info("No se encontraron logs para eliminar")
    else:
        logger.info(f"Total de logs eliminados: {eliminados}")


if __name__ == "__main__":
    pipeline_principal()
    limpia_logs_viejos()
    sys.exit()
