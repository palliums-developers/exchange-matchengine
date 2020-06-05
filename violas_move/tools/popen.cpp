#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <iostream>
#include <string>
#include <vector>

// FILE *f = popen("./output", "r");
// int d = fileno(f);
// fcntl(d, F_SETFL, O_NONBLOCK);

std::vector<std::string> read_lines(const char* file) {
  std::vector<std::string> v;

  FILE* fh = fopen(file, "r");
  char buf[4096];
  for(;;) {
    char* s = fgets(buf, sizeof(buf), fh);
    if(s == NULL) break;
    v.push_back(s);
  }
  return v;
}

std::vector<std::string> gaccounts;

int gen_accounts() {
  FILE * fp = popen("./cli.sh", "w");
  const char* s = "a c\n";
  fputs(s, fp);
  fputs(s, fp);
  fputs(s, fp);
  fputs(s, fp);
  fputs(s, fp);
  fflush(fp);
  pclose(fp);
}

std::string string_replace(std::string str, std::string a, std::string b) {
  std::string::size_type pos = 0;
  if((pos = str.find(a)) != std::string::npos) {
    str.replace(pos, a.length(), b);
  }
  if((pos = str.find(a)) != std::string::npos) {
    str.replace(pos, a.length(), b);
  }
  if((pos = str.find(a)) != std::string::npos) {
    str.replace(pos, a.length(), b);
  }
  return str;
}

std::string handle_line(std::string line) {
  line = string_replace(line,"@0", "0x"+gaccounts[0]);
  line = string_replace(line,"@1", "0x"+gaccounts[1]);
  line = string_replace(line,"@2", "0x"+gaccounts[2]);
  line = string_replace(line,"@3", "0x"+gaccounts[3]);
  line = string_replace(line,"@4", "0x"+gaccounts[4]);

  std::string::size_type pos = 0;

  if(line.empty() || line[0] == '#' || line[0] == ' ' || line[0] == '\n' ) {
    return "";
  }
  
  if((pos = line.find("exit")) != std::string::npos) {
    exit(1);
  }
  
  if((pos = line.find("sleep")) != std::string::npos) {
    std::string sub = line.substr(pos + strlen("sleep "));
    sub.pop_back();
    int num = atoi(sub.c_str());
    printf("sleep %d seconds", num);
    for(int i=0; i<num; ++i) {
      printf("."); fflush(NULL); sleep(1);
    }
    printf("\n");
    return "";
  }
  
  return line;
}

std::string red_color(const char* str) {
  char buf[8192];
  sprintf(buf, "\033[31m%s\033[0m", str);
  return buf;
}

std::string green_color(const char* str) {
  char buf[8192];
  sprintf(buf, "\033[32m%s\033[0m", str);
  return buf;
}

std::string purple_color(const char* str) {
  char buf[8192];
  sprintf(buf, "\033[35m%s\033[0m", str);
  return buf;
}

int do_job() {
  FILE * fp = popen("./cli.sh", "w");
  const char* s = "a c\n";
  fputs(s, fp);
  fputs(s, fp);
  fputs(s, fp);
  fputs(s, fp);
  fputs(s, fp);
  fflush(fp);
  fputs("a m 0 100 LBR \n", fp);
  fputs("a m 1 100 LBR \n", fp);
  fputs("a m 2 100 LBR \n", fp);
  fputs("a m 3 100 LBR \n", fp);
  fputs("a m 4 100 LBR \n", fp);
  fflush(fp);

  sleep(3);
  
  auto lines = read_lines("jobs.txt");

  for(int i=0; i<lines.size(); ++i) {
    auto line = handle_line(lines[i]);
    if(!line.empty()) {
      printf("%s: %s", green_color("command").c_str(), line.c_str());
      fputs(line.c_str(), fp);
      fflush(fp); sleep(1);
    }
  }
  
  pclose(fp);
}


			   
std::vector<std::string> extract_account_address() {

  std::vector<std::string> v;
  
  FILE* fh = fopen("a.log", "r");
  char buf[8192*2];
  fread(buf, 1, sizeof(buf), fh);
  fclose(fh);

  int n = strlen("#0 address ");
  char* s;
  
  s = strstr(buf, "#0"); 
  v.push_back(std::string(s+n, 32));
  s = strstr(buf, "#1"); 
  v.push_back(std::string(s+n, 32));
  s = strstr(buf, "#2"); 
  v.push_back(std::string(s+n, 32));
  s = strstr(buf, "#3"); 
  v.push_back(std::string(s+n, 32));
  s = strstr(buf, "#4"); 
  v.push_back(std::string(s+n, 32));

  return v;
}

int main(int argc, char* argv[]) {

  std::string type(argv[1]);
  
  if(type == "0") {
    system("rm -f client.mnemonic");
    system("./a.out 1 > a.log");
    gaccounts = extract_account_address();
    system(("cd move && ./b.sh " + gaccounts[0] + " && cd -").c_str());
    do_job();
  }
  
  if(type == "1")
    gen_accounts();
}
