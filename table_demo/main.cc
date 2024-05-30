#include <iostream>

#include "inlineskiplist.h"

struct Test {
    int i{0};
    int j{0};
};

int main(int argc, char* argv[]) {
    table::SkipList<int, Test> skiplist;
    skiplist.Insert(1, {2, 4});
    skiplist.Insert(3, {5, 4});
    skiplist.Insert(5, {7, 9});
    skiplist.Insert(7, {3, 9});
    skiplist.Insert(2, {7, 6});
    skiplist.Insert(9, {4, 3});
    Test* r;
    skiplist.Find(1, &r);
    std::cout << r->i << " " << r->j << std::endl;

    skiplist.Find(9, &r);
    std::cout << r->i << " " << r->j << std::endl;

    skiplist.Delete(5);

    return 0;
}
