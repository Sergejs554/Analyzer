# Mixea Analyzer (Railway ready)

Два режима:
1) CLI анализ локальных файлов
2) Flask API `/analyze?before=<url>&after=<url>`

## Быстрый старт (Railway)
- Залей репозиторий, в корне оставь эти файлы.
- Deploy on Railway (Nixpacks подтянет ffmpeg).
- Открой Logs — API поднимется на web-порту.

### Примеры
CLI:
```
python analyze_mastering.py --before before.mp3 --after after.mp3 --outdir report
```

API:
```
GET /analyze?before=https://.../before.mp3&after=https://.../after.mp3
```

Выход:
- `report.json`, `bands_1_3_octave.csv`, графики — в каталоге out (CLI).
- В API — JSON с метриками и подсказкой пресета.
