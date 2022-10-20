from src.main import my_fn


def test_my_fn():
    # confirm that my_fn simply sums two integers
    out = my_fn(1, 2)
    assert out == 3
