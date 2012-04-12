#include "RelationStatistics.h"

#include <map>
#include <set>
#include <sstream>
#include <iostream>

using std::map;
using std::set;
using std::endl;
using std::string;
using std::ostream;
using std::istream;

RelationStats::RelationStats () : numRows(){
  numRows = 0;
}

RelationStats::RelationStats (int _numRows) : numRows(_numRows){

}

RelationStats::~RelationStats () {
  attributes.clear();
}

void RelationStats::UpdateRowCount (int _numRows){
  numRows = _numRows;
}

void RelationStats::AddAttribute (string attr, int numDistinct){
  attributes[attr] = numDistinct;
}

RelationStats& RelationStats::operator= (RelationStats &rs){

  if (this != &rs){
    this->numRows = rs.numRows;
    this->attributes.clear();
    this->attributes = rs.attributes;
  }

  return *this;

}

ostream& operator<< (ostream& os, RelationStats &stats){

  map<string,int>::iterator iter = stats.attributes.begin();
  map<string,int>::iterator end = stats.attributes.end();

  os << stats.attributes.size() << endl;

  for (; iter != end; iter++){
    os << iter->first << endl << iter->second << endl;
  }

  return os;

}

istream& operator>> (istream& is, RelationStats &stats){

  int numIterations = 0;

  string line;
  std::stringstream s("");

    // Get the first line and put into the stringstream
  getline(is, line);
  s.str(line);

    // Read in an integer
  if (!(s >> numIterations)){
    numIterations = 0;
  }
  s.clear();
  s.str("");

  string attName;
  int numDistinct = 0;

    // Loop through each (attribute, numDistinct) pair
    // and store the result into the map
  for (int i = 0; i < numIterations; i++){

      // Get the attribute name
    getline(is, line);
    attName = line;

      // Get the number of distinct values
    getline(is, line);
    s.str(line);

    if (!(s >> numDistinct)){
      numDistinct = 0;
    }
    s.clear();
    s.str("");

      // Store the data
    stats.attributes[attName] = numDistinct;

  } // end for

  return is;

}

int RelationStats::operator[] (string attr){

  int result = 0;

  if (attributes.find(attr) == attributes.end()){
    result = -2;
  }
  else if (attributes[attr] == -1) {
    result = numRows;
  }
  else {
    result = attributes[attr];
  }

  return result;

}

int RelationStats::GetNumRows (){
  return numRows;
}




RelationSet::RelationSet () {

}

RelationSet::RelationSet (string relation) {
  joined.insert(relation);
}


RelationSet::~RelationSet () {

}

void RelationSet::AddRelationToSet (string relation){
  joined.insert(relation);
}

int RelationSet::Intersect (RelationSet rs){

  RelationSet temp;
  int numMatches = 0;

  set<string>::iterator it = rs.joined.begin();

  for (; it != rs.joined.end(); it++){

    if (this->joined.find(*it) != this->joined.end()){
      temp.AddRelationToSet(*it);
    }

  } // end for it

  int resultSize = temp.Size();

  if (resultSize == 0){
    numMatches = 0;
  }
  else if (resultSize == Size()){
    numMatches = resultSize;
  }
  else {
    numMatches = -1;
  }

  return numMatches;

}

void RelationSet::Merge (RelationSet rs){

  set<string>::iterator it = rs.joined.begin();

  for (; it != rs.joined.end(); it++){
    AddRelationToSet(*it);
  } // end for it

}

int RelationSet::Size (){
  return joined.size();
}

RelationSet& RelationSet::operator= (RelationSet &rs){

  if (this != &rs){
    this->joined.clear();
    this->joined = rs.joined;
  }

  return *this;

}
