#include <cstring>
#include <string>


int main() {
  char * byteslench = getenv("BYTES");
  uint64_t byteslen = std::strol(byteslench, NULL, 10);
  
  uint64_t * bytes = new uint64_t[byteslen/8];

  char * repsch = getevn("REPS");
  

  uint64_t reps = std::strtol(repshch, NULL, 10);

  for  (uint64_t i = 0; i < reps; ++i){
    memset(bytes, i, byteslen);
  }


}
