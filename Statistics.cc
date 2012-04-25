#include "Statistics.h"
#include "RelationStatistics.h"

#include <map>
#include <set>
#include <vector>
#include <string>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>

using std::map;
using std::set;
using std::clog;
using std::clog;
using std::endl;
using std::vector;
using std::string;
using std::ofstream;
using std::ifstream;
using std::stringstream;

Statistics::Statistics() {

  tables["n"] = "nation";
  tables["r"] = "region";
  tables["p"] = "part";
  tables["s"] = "supplier";
  tables["ps"] = "partsupp";
  tables["c"] = "customer";
  tables["o"] = "orders";
  tables["l"] = "lineitem";

}

Statistics::Statistics(Statistics &copyMe) {

  relations = copyMe.relations;
  relSets = copyMe.relSets;
//  sets = copyMe.sets;

  tables = copyMe.tables;

}

Statistics::~Statistics() {

}

Statistics& Statistics::operator= (Statistics &s){

  if (this != &s){
    relations = s.relations;
    relSets = s.relSets;
//    sets = s.sets;

    tables = s.tables;
  }

  return *this;

}

void Statistics::AddRel(char *relName, int numTuples) {

  string relation(relName);

  if (Exists(relation)){
    relations[relation].UpdateRowCount(numTuples);
  }
  else {

    RelationStats stats(numTuples);
    RelationSet set(relation);

    relations[relation] = stats;
    sets.push_back(set);
    relSets[relation] = set;

  }

}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {

  string relation(relName);
  string attribute(attName);

  if (Exists(relation)){
    relations[relation].AddAttribute(attribute, numDistincts);
  }

}

void Statistics::CopyRel(char *oldName, char *newName) {

  string oldRelation(oldName);
  string newRelation(newName);

  if (Exists(oldRelation)){

    relations[newRelation] = relations[oldRelation];

    RelationSet set(newName);
    sets.push_back(set);
    relSets[newRelation] = set;

  }

}

void Statistics::Read(char *fromWhere) {

  ifstream reader(fromWhere);

  if (reader.is_open()){

    map<string,RelationStats>::iterator iter;

    stringstream s;

    string line;
    string relation;

    int numStats = 0;
    int numSets = 0;

    int i = 0;

    if (reader.good()) {

      getline(reader, line);
      s.str(line);

      if (! (s >> numStats)){
	numStats = 0;
      }
      s.clear();
      s.str("");

      for (i = 0; i < numStats; i++){

	RelationStats stats;

	  // Get the first line
	getline(reader, line);

	  // If the first line is a newline, continue to the
	  // next line
	if (line.compare("") == 0){
	  getline(reader, line);
	}

	relation = line;

	  // Read in the statistics for this relation
	reader >> stats;

	  // Store the data
	relations[relation] = stats;	

      } // end for i

	// Get the first line
      getline(reader, line);

      while (line.compare("***") != 0){
	getline(reader, line);
      }

      getline(reader, line);

      getline(reader, line);
      s.str(line);

      if (! (s >> numSets)){
	numSets = 0;
      }

	// Get the first line
      getline(reader, line);

      for (i = 0; i < numSets; i++){

	RelationSet rs;
	vector<string> setRel;

	  // Read in the set for this relation
	reader >> rs;
	rs.GetRelations(setRel);
	sets.push_back(rs);

	vector<string>::iterator it = setRel.begin();

	for (; it != setRel.end(); it++){
	  relSets[*it] = rs;
	}

      } // end for i


    } // end while iter

    reader.close();

  } // end if writer.is_open()

}

void Statistics::Write(char *fromWhere) {

  ofstream writer(fromWhere);

  if (writer.is_open() && sets.size() > 0){

      // Get an iterator to all the RelationStats
    map<string,RelationStats>::iterator iterRel;

    writer << relations.size() << endl << endl;

      // Print out the statistics to the stream
    for (iterRel = relations.begin(); iterRel != relations.end(); iterRel++){
      if (writer.good()){
	writer << iterRel->first << endl << iterRel->second << endl;
      }
    } // end while iter


    writer << "***\n" << endl;
    writer << sets.size() << endl << endl;

      // Print out the sets to the stream
    int index = 0;
    int size = (int) sets.size();

    for (; index < size; index++) {
	writer << sets[index] << endl;
    }

    writer.close();

  } // end if writer.is_open()

}

void Statistics::Apply(struct AndList *parseTree,
			char *relNames[], int numToJoin) {

  RelationSet set;
  vector<int> indexes;

  vector<RelationSet> copy;

    // Create a new RelationSet represented by relNames
  for (int i = 0; i < numToJoin; i++){
    set.AddRelationToSet(relNames[i]);
  }

  double joinEstimate = 0.0;
  joinEstimate = ParseJoin(parseTree);

    // Run the estimation. We don't care about the value!
  double estimate = Guess(parseTree, set, indexes, joinEstimate);

  int index = 0;
  int oldSize = sets.size();
  int newSize = oldSize - (int) indexes.size() + 1;


    // If relNames spans multiple sets, consolidate them
  if (indexes.size() > 1){


      // Only copy over sets not spanned by set
    for (int i = 0; i < oldSize; i++){
      if (i == indexes[index]){
	index++;
      }
      else {
	copy.push_back(sets[i]);
      }
    } // end for int i

      // Clear the global sets list
    sets.clear();

      // Perform the copy
    for (int i = 0; i < newSize-1; i++){
      sets.push_back(copy[i]);
    }

    set.UpdateNumTuples(estimate);
    sets.push_back(set);

    copy.clear();

    vector<string> relations;
    set.GetRelations(relations);

    for (int i = 0; i < set.Size(); i++){
      relSets[relations[i]] = set;
    }

  } // end if indexes.size()

}

double Statistics::Estimate(struct AndList *parseTree,
			    char **relNames, int numToJoin) {

  double estimate = 0.0;

  RelationSet set;
  vector<int> indexes;

    // Create a RelationSet represented by relNames
  for (int i = 0; i < numToJoin; i++){
    set.AddRelationToSet(relNames[i]);
  }

  double joinEstimate = 0.0;
  joinEstimate = ParseJoin(parseTree);

  clog << "Join Estimate was " << joinEstimate << endl;

    // Get the estimate
  estimate = Guess(parseTree, set, indexes, joinEstimate);
  //clog << "Guess was " << estimate << endl;

  return estimate;

}

double Statistics::Guess(struct AndList *parseTree, RelationSet toEstimate,
			    vector<int> &indexes, double joinEstimate){

  double estimate = 0.0;
/*
    // If parse tree has unknown attributes, exit the program
  if (!CheckParseTree(parseTree)){
    clog << "BAD: attributes in parseTree do not match any given relation!"
	 << endl;
    exit(1);
  }
*/
    // If the given set can be found in the existing sets, create an estimate
  if (CheckSets(toEstimate, indexes)) {
    estimate = GenerateEstimate(parseTree, joinEstimate);
  }

  return estimate;

}

bool Statistics::CheckSets(RelationSet toEstimate, vector<int> &indexes){

  int numRelations = 0;
  
  int intersect = 0;
  int index = 0;

  int size = (int) sets.size();

    // Iterate through all the sets
  for (; index < size; index++){

      // Compute the intersect value
    intersect = sets[index].Intersect(toEstimate);

      // If the computation returned -1, stop - the join is not feasible
    if (intersect == -1){
      indexes.clear();
      numRelations = 0;
      break;
    }

      // Else if computation returned a positive int, keep track of
      // how many relations in the set have been matched
    else if (intersect > 0) {

      numRelations += intersect;
      indexes.push_back(index);

      if (numRelations == toEstimate.Size()){
	break;
      } // end if numRelations

    } // else if intersect

  } // end for index

  return (numRelations > 0);

}

bool Statistics::CheckParseTree(struct AndList *parseTree){

  if (parseTree){

    struct AndList *curAnd = parseTree;
    struct OrList *curOr;
    struct ComparisonOp *curOp;

      // Cycle through the ANDs
    while (curAnd){

      curOr = curAnd->left;

	// Cycle through the ORs
      while (curOr){

	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->left){
	   // clog << curOp->left->value;
	  }


	  switch (curOp->code){
	    case 1:
	//      clog << " < ";
	      break;

	    case 2:
	//      clog << " > ";
	      break;

	    case 3:
	//      clog << " = ";
	      break;

	  } // end switch curOp->code

	  if (curOp->right){
	//    clog << curOp->right->value;
	  }


	} // end if curOp

	if (curOr->rightOr){
	//  clog << " OR ";
	}

	curOr = curOr->rightOr;

      } // end while curOr

      if (curAnd->left){
	//clog << ")";
      }

      if (curAnd->rightAnd) {
	//clog << " AND ";
      }

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree


  return true;

}

double Statistics::ParseJoin(struct AndList *parseTree){

  double value = 0.0;
  double dummy = 0.0;

  if (parseTree){

    struct AndList *curAnd = parseTree;
    struct OrList *curOr;
    struct ComparisonOp *curOp;

    bool stop = false;

    string relation1, relation2, attribute1, attribute2;
    RelationStats r1, r2;

      // Cycle through the ANDs
    while (curAnd && !stop){
      curOr = curAnd->left;

	// Cycle through the ORs
      while (curOr && !stop){
	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->code == EQUALS && curOp->left->code == curOp->right->code){

	    stop = true;

	      // Get the relation names and the attributes
	    ParseRelationAndAttribute(curOp->left, relation1, attribute1);
	    ParseRelationAndAttribute(curOp->right, relation2, attribute2);

	      // Get the relevant statistics
	    r1 = relations[relation1];
	    r2 = relations[relation2];

	      // join algorithm: |A|*|B|* (1/max(v(a),v(b)))
	    value = GetRelationCount(relation1, dummy) *
		    GetRelationCount(relation2, dummy);
//clog << "r1 " << relation1 << " :" << GetRelationCount(relation1, dummy) << endl;
//clog << "r2 " << relation2 << " :" << GetRelationCount(relation2, dummy) << endl;
	    if (r1[attribute1] >= r2[attribute2]){
	      value /= (double) r1[attribute1];
	    }
	    else {
	      value /= (double) r2[attribute2];
	    }

	  }

	} // end if curOp

	curOr = curOr->rightOr;

      } // end while curOr

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree


  return value;

}


double Statistics::GenerateEstimate(struct AndList *parseTree,
		      double joinEstimate){

  double estimate = 0.0;
  clog << "Estimate is 0.0" << endl;
  double orEstimate = 0.0;
  double tempOrEstimate = 0.0;
  bool independentOR = false;

  int numIterations = 0;

  struct AndList *curAnd = parseTree;
  struct OrList *curOr;
  struct ComparisonOp *curOp;

  string relation1;
  string attribute1;
  string relation2;
  string attribute2;
  RelationStats r1;
  RelationStats r2;

    // Cycle through the ANDs
  while (curAnd){

    curOr = curAnd->left;

    orEstimate = 0.0;

    if (curOr->rightOr){

      tempOrEstimate = 0.0;
      independentOR = CheckIndependence(curOr);

      if (independentOR){
	tempOrEstimate = 1.0;
      }

	// Cycle through the ORs
      while (curOr){

	curOp = curOr->left;

        Operand *op = curOp->left;

	if (op->code != NAME){
	  op = curOp->right;
	}

	  // Get the relation name and the attribute
	ParseRelationAndAttribute(op, relation1, attribute1);
	r1 = relations[relation1];

	  // OR is not independent
	if (!independentOR){
		clog << "Independent Or: " << endl;
	    // selection = algorithm: |R| / v(a)
	  if (curOp->code == EQUALS){
	    clog << "Estimating SELECTION: |R| / v(a)" << endl;
	    clog << "TempOr was " << tempOrEstimate << endl;
	    tempOrEstimate += GetRelationCount(relation1, joinEstimate)
				/ (double) r1[attribute1];
	    clog << "TempOr is now " << tempOrEstimate << endl;
	  }

	    // selection < or > algorithm: |R| / 3
	  else {
	    clog << "Estimating SELECTION: |R| / 3" << endl;
	    clog << "TempOr was " << tempOrEstimate << endl;
	    tempOrEstimate += GetRelationCount(relation1, joinEstimate) / 3.00;
	    clog << "TempOr is now " << tempOrEstimate << endl;
	  }
	}

	  // OR is independent
	else {
		clog << "Dependent Or: " << endl;
	  if (curOp->code == EQUALS){
	    clog << "Equality SELECTION: 1 - (1 / r1[attribute])" << endl;
	    tempOrEstimate *= 1.00 - (1.00 / (double) r1[attribute1]);
	  }

	  else {
	    clog << "Inequality SELECTION: 1 - (1 / 3)" << endl;
	    tempOrEstimate *= 1.00 - (1.00 / 3.00);
	  }


	} // end else


	curOr = curOr->rightOr;

      } // end while curOr

      if (independentOR){
	tempOrEstimate = (double) (1.00 - tempOrEstimate);
      }

      orEstimate = tempOrEstimate;

    } // end if curOr->rightOr

      // Else we have only one condition to deal with
    else {

	// Get the operation
      curOp = curOr->left;


	// If both left and right operands are attributes, we have a join!
      if (curOp->left->code == curOp->right->code){

	orEstimate = -1.0;
	clog << "How did I get here? What is supposed to happen now? NICCCCK!?!" << endl;
/*
	  // Get the relation names and the attributes
	ParseRelationAndAttribute(curOp->left, relation1, attribute1);
	ParseRelationAndAttribute(curOp->right, relation2, attribute2);

	  // Get the relevant statistics
	r1 = relations[relation1];
	r2 = relations[relation2];

	  // join algorithm: |A|*|B|* (1/max(v(a),v(b)))
	orEstimate = GetRelationCount(relation1) * GetRelationCount(relation2);

	if (r1[attribute1] >= r2[attribute2]){
	  orEstimate /= (double) r1[attribute1];
	}
	else {
	  orEstimate /= (double) r2[attribute2];
	}
*/
      } // end if curOp->left->code

	// Else we are parsing a single OR
      else {
	clog << "Parsing single or" << endl;
        Operand *op = curOp->left;

	if (op->code != NAME){
	  op = curOp->right;
	}

	  // Get the relation name and the attribute
	ParseRelationAndAttribute(op, relation1, attribute1);
	r1 = relations[relation1];

	  // selection = algorithm: |R| / v(a)
	if (curOp->code == EQUALS){
	  clog << "SELECTION: |R|/v(a)" << endl;
	  clog << "Before: " << orEstimate << endl;
	  orEstimate = GetRelationCount(relation1, joinEstimate)
			  / (double) r1[attribute1];
	  clog << "After: " << orEstimate << endl;
	}

	  // selection < or > algorithm: |R| / 3
	else {
	  clog << "SELECTION: |R|/3" << endl;
	  clog << "Before: " << orEstimate << endl;
	  orEstimate = GetRelationCount(relation1, joinEstimate) / 3.00;
	  clog << "After: " << orEstimate << endl;
	}

      } // end else

    } // end else

    if (orEstimate != -1.0){
      if (estimate == 0.0){
	clog << "Setting estimate to orEstimate" << endl;
	estimate = orEstimate;
      }
      else {
	clog << "Setting estimate to est * orEst" << endl;
	clog << "Est: " << estimate << endl;
	clog << "OrEst: " << orEstimate << endl;
	estimate *= orEstimate;
	clog << "New Est: " << estimate << endl;
      }
    }

    curAnd = curAnd->rightAnd;

    numIterations++;

  } // end while curAnd

  if (numIterations == 1 && estimate == 0.0 && joinEstimate > 0.0){
    estimate = joinEstimate;
    clog << "Setting estimate as joinEstimate " << estimate << endl;
  }

  clog << "Estimate at end is " << estimate << endl;
  return estimate;

}

double Statistics::GetRelationCount(string relation, double joinEstimate){

  double count = 0.0;

  count = joinEstimate;

  if (count == 0.0){
    count = relSets[relation].GetNumTuples();
  }

  if (count == -1.0){
    count = (double) relations[relation].GetNumRows();
  }

  return count;

}

void Statistics::ParseRelationAndAttribute(struct Operand *op,
		  string &relation, string &attribute) {

    string value(op->value);
    string rel;
    stringstream s;

    int i = 0;

    while (value[i] != '_'){

      if (value[i] == '.'){
	relation = s.str();
	break;
      }

      s << value[i];

      i++;

    }

    if (value[i] == '.'){
      attribute = value.substr(i+1);
    }
    else {
      attribute = value;
      rel = s.str();
      relation = tables[rel];
    }

}


bool Statistics::Exists(string relation) {
  return (relations.find(relation) != relations.end());
}


bool Statistics::CheckIndependence (struct OrList *parseTree){

    // Method checks to see if the ORs in this are independent or not.
    // Returns false if they are, true if they are not. If all
    // attributes in list are the same, it is not an independent
    // or list.

    // Independent: Attributes in list are different.
    // Dependent: Attributes in list are the same

  struct OrList *curOr = parseTree;

  string lName;
  vector<string> checkVec;

  while (curOr){

      // When inside an OR, we want to start keeping track of what we
      // see. If we find an attribute that's not the same, we need to
      // mark that or list as dependent.

    if (checkVec.size() == 0){
      lName = curOr->left->left->value;
      checkVec.push_back(lName);
    }
    else{
      if(checkVec[0].compare(curOr->left->left->value) != 0){
	lName = curOr->left->left->value;
	checkVec.push_back(lName);
      }
    }

    curOr = curOr->rightOr;

  } // end while curOr

   // Dependent will have checkVec size 1, independent will have size > 1
  return (checkVec.size() > 1);

}
