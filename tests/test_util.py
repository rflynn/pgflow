
import unittest
from unittest import skip

from pgflow.util import flatten1, flatten


class TestFlatten1(unittest.TestCase):

    def test_empty(self):
        self.assertEqual([], flatten1([]))

    @skip("hmmm")
    def test_list(self):
        self.assertEqual([1, 2], flatten1([1, 2]))

    def test_flatter(self):
        self.assertEqual([1, 2], flatten1([[1], [2]]))


class TestFlatten(unittest.TestCase):

    def test_empty(self):
        self.assertEqual([], flatten([]))

    def test_empty_multilevel(self):
        self.assertEqual([], flatten([[[[[]]]]]))

    def test_already_flat(self):
        self.assertEqual([0], flatten([0]))

    def test_multilevel1(self):
        self.assertEqual([0], flatten([[0]]))

    def test_jagged(self):
        self.assertEqual([0, 1, 2], flatten([0, [1, [2]]]))

    def test_string_list(self):
        self.assertEqual(['ab'], flatten(['ab']))

    def test_string_list_deeper(self):
        self.assertEqual(['ab'], flatten([['ab']]))

    def test_string_list_deeper(self):
        self.assertEqual(['a', 'b'], flatten(['a','b']))
