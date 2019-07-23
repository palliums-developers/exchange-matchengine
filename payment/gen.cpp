#include <stdio.h>
#include <iostream>
#include <string>
#include <vector>

std::vector<std::string> v = {
  "l_id","i_type","l_from","l_to","d_amount","i_timestamp","s_withdraw_addr","s_recharge_utxo","s_withdraw_utxo","d_withdraw_fee","i_utxo_confirmed"
};

void func_create(std::string str)
{
  auto type = str.substr(0, 1);
  auto name = str.substr(2);

  auto itemp = "if(kvs.count(\"%s\")) \n o->_%s = atoi(kvs[\"%s\"].c_str());";
  auto ltemp = "if(kvs.count(\"%s\")) \n o->_%s = atol(kvs[\"%s\"].c_str());";
  auto dtemp = "if(kvs.count(\"%s\")) \n o->_%s = atof(kvs[\"%s\"].c_str());";
  auto stemp = "if(kvs.count(\"%s\")) \n o->_%s = kvs[\"%s\"];";

  std::string temp;
  if(type == "i") temp = itemp;
  if(type == "l") temp = ltemp;
  if(type == "d") temp = dtemp;
  if(type == "s") temp = stemp;

  temp += "\n";
  
  printf(temp.c_str(), name.c_str(), name.c_str(), name.c_str());
}

void func_tomap(std::string str)
{
  auto type = str.substr(0, 1);
  auto name = str.substr(2);

  std::string temp = "v[\"%s\"] = std::to_string(_%s);";
  temp += "\n";
  
  printf(temp.c_str(), name.c_str(), name.c_str());
}

void func_tojson(std::string str)
{
  auto type = str.substr(0, 1);
  auto name = str.substr(2);

  std::string temp = "str += json(\"%s\", _%s, false);";
  temp += "\n";
  
  printf(temp.c_str(), name.c_str(), name.c_str());
}



int main()
{
  for(auto a : v)
    std::cout << a << " ";
  std::cout << "\n";

  for(auto a: v)
    func_tojson(a);

  return 0;
}
