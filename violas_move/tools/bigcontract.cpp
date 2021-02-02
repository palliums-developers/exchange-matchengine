#include <iostream>
#include <stdio.h>

int main(int argc, char* argv[]) {

  // address 0x1 {
  //   module ViolasBank {
  //     resource struct T {
  //     index: u64,
  // 	  value: u64,
  // 	  }
  //   }
  // }

  int n = atoi(argv[1]);
  printf("address 0x1 {\n");
  printf("module ViolasBank {\n");

  for (int i=0; i<n; ++i) {
    printf("  resource struct T%d {\n", i);
    printf("    index: u64,\n");
    printf("    value: u64,\n");
    printf("  }\n");
  }
  
  printf("}\n");
  printf("}\n");
}
