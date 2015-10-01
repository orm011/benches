#include <cstring>
#include <string>
#include <stdlib.h>

int main() {
  char * byteslench = getenv("BYTES");
  uint64_t byteslen = std::strtol(byteslench, NULL, 10);
  uint64_t * bytes = new uint64_t[byteslen/8];

  char * repsch = getenv("REPS");
  uint64_t reps = std::strtol(repsch, NULL, 10);

  for  (uint64_t i = 0; i < reps; ++i){
    memset(bytes, i, byteslen);
  }
}
