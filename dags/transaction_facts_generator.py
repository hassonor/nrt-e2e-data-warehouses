from datetime import datetime


start_date = datetime(2024, 12, 12)
default_args = {
    'owner': 'orhasson',
    'depends_on_past': False,
    'backfill': False,
}
