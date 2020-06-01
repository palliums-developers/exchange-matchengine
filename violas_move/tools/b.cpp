#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>

#include <iostream>
#include <vector>
#include <string>


using namespace std;

void GetFileNames(string path,vector<string>& filenames)
{
    DIR *pDir;
    struct dirent* ptr;
    if(!(pDir = opendir(path.c_str())))
        return;
    while((ptr = readdir(pDir))!=0) {
      if (strcmp(ptr->d_name, ".") != 0 && strcmp(ptr->d_name, "..") != 0) {
	if ((ptr->d_type & DT_REG) != 0)
	  filenames.push_back(path + "/" + ptr->d_name);
      }
    }
    closedir(pDir);
}


int main() {

  vector<string> v;
  GetFileNames("/home/ops/lmf/move/stdlib/modules", v);


  string dep;
  for(int i=0; i<v.size(); ++i) 
    dep += v[i] + " ";
  
  printf("../move-build bank.move -s 0x$addr -d %s \n\n", dep.c_str());
  printf("mv move_build_output/modules/0_ViolasBank.mv bank.mv \n\n");
  
  dep += "bank.move";

  vector<string> scripts {
    "publish",
      "register_libra_token",
      "enter_bank",
      "exit_bank",
      "update_price",
      "update_collateral_factor",
      "lock",
      "redeem",
      "borrow",
      "repay_borrow",
      "liquidate_borrow",
      };

  for(int i=0; i<scripts.size(); ++i) {
    printf("../move-build %s.move -s 0x$addr -d %s \n\n", scripts[i].c_str(), dep.c_str());
    printf("mv move_build_output/scripts/main.mv %s.mv \n\n", scripts[i].c_str());
  }

  //printf("../create_token.move -s 0x$addr -d %s", dep.c_str());
  
  return 0;
}
