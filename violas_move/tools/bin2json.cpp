#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <string>

int main(int argc, char* argv[])
{
  char* file = argv[1];
  
  FILE* fh = fopen(file, "r");
  char buf[8192*4];
  int flen = fread(buf, 1, sizeof(buf), fh);

  std::string file2 = file;
  file2 += ".1";
  
  FILE* fh2 = fopen(file2.c_str(), "w");
  const char* str = "{\"code\":[";
  fwrite(str, 1, strlen(str), fh2);

  for(int i=0; i<flen; i++) {
    if(i != 0)
      fwrite(",", 1, 1, fh2);
    char tb[80];
    sprintf(tb, "%d", (int)(unsigned char)buf[i]);
    fwrite(tb, 1, strlen(tb), fh2);
  }
  
  str = "],\"args\":[]}";
  fwrite(str, 1, strlen(str), fh2);
  
  fclose(fh);
  fclose(fh2);
  
}
