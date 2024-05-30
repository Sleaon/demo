#include <rocksdb/options.h>

#include <iostream>
#include <string>

#include "rocksdb/db.h"
int main(int argc, char* argv[]) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    std::string path = "./test";
    auto s = rocksdb::DB::Open(options, path, &db);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
    }
    rocksdb::WriteOptions wo;
    wo.sync = true;

    db->Put(wo, "12314", "asdasdas");
    db->Put(wo, "122314", "asdasdas");
    db->Put(wo, "142314", "asdasdas");
    db->Put(wo, "162314", "as23dasdas");
    db->Put(wo, "102314", "asdasdas");

    return 0;
}
