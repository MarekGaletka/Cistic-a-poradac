from __future__ import annotations

from godmode_media_library.autolabel_people import _resize_if_needed


class _FakeArray:
    """Minimal mock that simulates a numpy array with a .shape attribute."""

    def __init__(self, shape: tuple[int, ...]):
        self.shape = shape


class _FakeNp:
    """Minimal mock for numpy module used by _resize_if_needed."""

    def array(self, obj):
        return obj


class _FakeImage:
    """Minimal mock for PIL.Image used by _resize_if_needed."""

    def __init__(self, size: tuple[int, int]):
        self._size = size

    def resize(self, new_size: tuple[int, int]):
        return _FakeImage(new_size)

    @classmethod
    def fromarray(cls, arr):
        shape = getattr(arr, "shape", (100, 100))
        return cls((shape[1], shape[0]))


def test_resize_if_needed_no_resize():
    """Small array should be returned unchanged."""
    arr = _FakeArray(shape=(100, 200, 3))
    result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
    # max(100, 200) = 200, which is <= 1600 — no resize needed
    assert result is arr
    assert scale == 1.0


def test_resize_if_needed_large_image():
    """Large array should be resized."""
    arr = _FakeArray(shape=(3200, 4800, 3))
    np_mod = _FakeNp()
    result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=np_mod, pil_image_cls=_FakeImage)
    # max(3200, 4800) = 4800 > 1600 — should be resized
    # result will be whatever _FakeNp.array returns from the resized image
    assert result is not arr
    assert scale < 1.0


def test_resize_if_needed_no_shape():
    """Object without proper shape should be returned as-is."""
    obj = "not_an_array"
    result, scale = _resize_if_needed(obj, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
    assert result is obj
    assert scale == 1.0


def test_resize_if_needed_1d_shape():
    """1D shape (less than 2 dimensions) should be returned as-is."""
    arr = _FakeArray(shape=(100,))
    result, scale = _resize_if_needed(arr, max_dimension=1600, np_mod=_FakeNp(), pil_image_cls=_FakeImage)
    assert result is arr
    assert scale == 1.0
