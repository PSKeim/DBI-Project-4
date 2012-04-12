#include "RelationStatistics.h" 
#include <fstream>
#include <iostream>

using namespace std;

int main(int argc, char **argv){

  int size1 = 5;

  RelationStats stats1(size1);
  RelationStats stats2, stats3;

  size1 += 2;
  stats1.UpdateRowCount(size1);

  cout << "Testing UpdateRowCount and << operator, stats1....\n" << endl;

  stats1.AddAttribute("att1", -1);
  stats1.AddAttribute("att2", 20);
  stats1.AddAttribute("att3", -1);
  stats1.AddAttribute("att4", 5);
  stats1.AddAttribute("att5", -1);
  stats1.AddAttribute("att6", 7);
  stats1.AddAttribute("att7", 12);

  cout << stats1 << endl;
 

  cout << "Testing = operator, stats2....\n" << endl;
  stats2 = stats1;
  cout << stats2 << endl;

  cout << "Testing [] operator ....\n" << endl;
  cout << "stats2[att6] = " << stats2["att6"] << " and should equal 7" << endl;
  cout << "stats2[att8] = " << stats2["att8"] << " and should equal -2" << endl;

  stats2.AddAttribute("att8", 64);
  cout << "stats2[att8] = " << stats2["att8"] << " and should equal 64" << endl;

  cout << "Testing >> operator,stats3 ....\n" << endl;
  
  ifstream reader("relation.txt");

  if (reader.is_open()){
    reader >> stats3;
  }

  reader.close();

  cout << stats3 << endl;

}
