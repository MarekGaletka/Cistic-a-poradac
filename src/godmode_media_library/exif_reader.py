"""Native EXIF/metadata reader using Pillow — reduces ExifTool dependency.

Reads DateTimeOriginal, camera make/model, and GPS from JPEG/TIFF/PNG.
Falls back gracefully when Pillow is not installed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_EXIF_EXTS = {
    "jpg", "jpeg", "tiff", "tif", "png", "webp",
    "heic", "heif", "avif", "dng", "cr2", "nef", "arw",
}

# Standard EXIF tag IDs
_TAG_MAKE = 0x010F
_TAG_MODEL = 0x0110
_TAG_DATETIME_ORIGINAL = 0x9003
_TAG_DATETIME_DIGITIZED = 0x9004
_TAG_DATETIME = 0x0132
_TAG_IMAGE_WIDTH = 0xA002
_TAG_IMAGE_HEIGHT = 0xA003
_TAG_GPS_INFO = 0x8825

# GPS sub-tag IDs
_GPS_LATITUDE_REF = 1
_GPS_LATITUDE = 2
_GPS_LONGITUDE_REF = 3
_GPS_LONGITUDE = 4
_GPS_ALTITUDE_REF = 5
_GPS_ALTITUDE = 6


@dataclass
class ExifMeta:
    """Extracted EXIF metadata."""

    date_original: str | None = None
    camera_make: str | None = None
    camera_model: str | None = None
    image_width: int | None = None
    image_height: int | None = None
    gps_latitude: float | None = None
    gps_longitude: float | None = None
    gps_altitude: float | None = None


def can_read_exif(ext: str) -> bool:
    """Return True if ext (without dot) is readable by native EXIF reader."""
    return ext.lower().lstrip(".") in _EXIF_EXTS


def read_exif(path: Path) -> ExifMeta | None:
    """Read EXIF metadata from an image file using Pillow.

    Returns ExifMeta or None if Pillow is not available or file cannot be read.
    """
    try:
        from PIL import Image
        from PIL.ExifTags import IFD
    except ImportError:
        logger.debug("Pillow not installed, cannot read EXIF")
        return None

    # Register HEIC/HEIF support if available
    ext = path.suffix.lower().lstrip(".")
    if ext in ("heic", "heif", "avif"):
        try:
            import pillow_heif
            pillow_heif.register_heif_opener()
        except ImportError:
            logger.debug("pillow-heif not installed, cannot read %s EXIF", ext)
            return None

    meta = ExifMeta()

    try:
        with Image.open(path) as img:
            meta.image_width = img.width
            meta.image_height = img.height

            exif_data = img.getexif()
            if not exif_data:
                return meta

            # Camera info from root IFD
            meta.camera_make = _clean_string(exif_data.get(_TAG_MAKE))
            meta.camera_model = _clean_string(exif_data.get(_TAG_MODEL))

            # DateTimeOriginal from EXIF IFD
            try:
                exif_ifd = exif_data.get_ifd(IFD.Exif)
            except (KeyError, AttributeError):
                exif_ifd = {}

            date_original = exif_ifd.get(_TAG_DATETIME_ORIGINAL)
            if date_original is None:
                date_original = exif_ifd.get(_TAG_DATETIME_DIGITIZED)
            if date_original is None:
                date_original = exif_data.get(_TAG_DATETIME)
            meta.date_original = _clean_string(date_original)

            # Image dimensions from EXIF (override PIL's if available)
            exif_w = exif_ifd.get(_TAG_IMAGE_WIDTH)
            exif_h = exif_ifd.get(_TAG_IMAGE_HEIGHT)
            if exif_w and exif_h:
                try:
                    meta.image_width = int(exif_w)
                    meta.image_height = int(exif_h)
                except (ValueError, TypeError):
                    pass

            # GPS info
            try:
                gps_ifd = exif_data.get_ifd(IFD.GPSInfo)
            except (KeyError, AttributeError):
                gps_ifd = {}

            if gps_ifd:
                _parse_gps(gps_ifd, meta)

    except (OSError, ValueError, KeyError, AttributeError):
        logger.debug("Cannot read EXIF from %s", path)
        return None

    return meta


def _parse_gps(gps_ifd: dict, meta: ExifMeta) -> None:
    """Parse GPS data from EXIF GPS IFD."""
    lat = gps_ifd.get(_GPS_LATITUDE)
    lat_ref = gps_ifd.get(_GPS_LATITUDE_REF)
    lon = gps_ifd.get(_GPS_LONGITUDE)
    lon_ref = gps_ifd.get(_GPS_LONGITUDE_REF)

    if lat and lon:
        try:
            meta.gps_latitude = _dms_to_decimal(lat, lat_ref)
            meta.gps_longitude = _dms_to_decimal(lon, lon_ref)
        except (ValueError, TypeError, ZeroDivisionError):
            pass

    alt = gps_ifd.get(_GPS_ALTITUDE)
    alt_ref = gps_ifd.get(_GPS_ALTITUDE_REF, 0)
    if alt is not None:
        try:
            alt_val = float(alt)
            if alt_ref == 1:
                alt_val = -alt_val
            meta.gps_altitude = alt_val
        except (ValueError, TypeError):
            pass


def _dms_to_decimal(dms: tuple, ref: str | None) -> float:
    """Convert (degrees, minutes, seconds) tuple to decimal degrees."""
    degrees = float(dms[0])
    minutes = float(dms[1])
    seconds = float(dms[2])
    decimal = degrees + minutes / 60 + seconds / 3600
    if ref in ("S", "W"):
        decimal = -decimal
    return round(decimal, 8)


def _clean_string(value: object) -> str | None:
    """Clean EXIF string value: strip nulls and whitespace."""
    if value is None:
        return None
    s = str(value).strip().rstrip("\x00")
    return s if s else None
