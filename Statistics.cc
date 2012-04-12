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
using std::cout;
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
  tables["o"] = "order";
  tables["l"] = "lineitem";


}

Statistics::Statistics(Statistics &copyMe) {

  relations.clear();
  relations = copyMe.relations;

  tables = copyMe.tables;

}

Statistics::~Statistics() {

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
  }

}

void Statistics::Read(char *fromWhere) {

  ifstream reader(fromWhere);

  if (reader.is_open()){

    map<string,RelationStats>::iterator iter;

    string line;
    string relation;
    RelationStats stats;

    while (reader.good()) {

	// Get the first line
      getline(reader, line);

        // If the first line is a newline, continue to the
	// next line
      if (line.compare("\n") == 0){
	getline(reader, line);
      }

	// Get the relation name
      relation = line;

	// Read in the statistics for this relation
      reader >> stats;

	// Store the data
      relations[relation] = stats;

    } // end while iter

    reader.close();

  } // end if writer.is_open()

}

void Statistics::Write(char *fromWhere) {

  ofstream writer(fromWhere);

  if (writer.is_open()){

      // Get an iterator to all the RelationStats
    map<string,RelationStats>::iterator iter;

      // Print out the data to the stream
    for (iter = relations.begin(); iter != relations.end(); iter++){
      if (writer.good()){
	writer << iter->first << endl << iter->second << endl;
      }
    } // end while iter

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
    set.AddRelationToSet((*relNames) + i);
  }

    // Run the estimation. We don't care about the value!
  Estimate(parseTree, set, indexes);

  int index = indexes[0];
  int newSize = sets.size() - indexes.size();

    // If relNames spans multiple sets, consolidate them
  if (indexes.size() > 1){

      // Only copy over sets not spanned by set
    for (int i = 0; i < newSize; i++){
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
    for (int i = 0; i < newSize; i++){
      sets[i] = copy[i];
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
    set.AddRelationToSet((*relNames) + i);
  }

    // Get the estimate
  estimate = Estimate(parseTree, set, indexes);

  return estimate;

}

double Statistics::Estimate(struct AndList *parseTree, RelationSet toEstimate,
			    vector<int> &indexes){

  double estimate = 0.0;
/*
    // If parse tree has unknown attributes, exit the program
  if (!CheckParseTree(parseTree)){
    cout << "BAD: attributes in parseTree do not match any given relation!"
	 << endl;
    exit(1);
  }
*/
    // If the given set can be found in the existing sets, create an estimate
  if (CheckSets(toEstimate, indexes)) {
    estimate = GenerateEstimate(parseTree);
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

      if (curAnd->left){
	cout << "(";
      }

	// Cycle through the ORs
      while (curOr){

	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->left){
	    cout << curOp->left->value;
	  }


	  switch (curOp->code){
	    case 1:
	      cout << " < ";
	      break;

	    case 2:
	      cout << " > ";
	      break;

	    case 3:
	      cout << " = ";
	      break;

	  } // end switch curOp->code

	  if (curOp->right){
	    cout << curOp->right->value;
	  }


	} // end if curOp

	if (curOr->rightOr){
	  cout << " OR ";
	}

	curOr = curOr->rightOr;

      } // end while curOr

      if (curAnd->left){
	cout << ")";
      }

      if (curAnd->rightAnd) {
	cout << " AND ";
      }

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree


  return true;

}

double Statistics::GenerateEstimate(struct AndList *parseTree){

  double estimate = -1.0;

  double orEstimate = 0.0;
  double tempOrEstimate = 0.0;
  bool independentOR = true;

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

      tempOrEstimate = 1.0;

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
	independentOR = CheckIndependence(curOr);
	  // OR is not independent
	if (!independentOR){

	    // selection = algorithm: |R| / v(a)
	  if (curOp->code == EQUALS){
	    tempOrEstimate += (double) r1.GetNumRows()
				/ (double) r1[attribute1];
	  }

	    // selection < or > algorithm: |R| / 3
	  else {
	    tempOrEstimate += (double) r1.GetNumRows() / 3.00;
	  }
	}

	  // OR is independent
	else {

	  if (curOp->code == EQUALS){
	    tempOrEstimate *= 1.00 - (1.00 / (double) r1[attribute1]);
	  }

	  else {
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

	  // Get the relation names and the attributes
	ParseRelationAndAttribute(curOp->left, relation1, attribute1);
	ParseRelationAndAttribute(curOp->right, relation2, attribute2);

	  // Get the relevant statistics
	r1 = relations[relation1];
	r2 = relations[relation2];

	  // join algorithm: |A|*|B|* (1/max(v(a),v(b)))
	orEstimate = (double) r1.GetNumRows() * (double) r2.GetNumRows();

	if (r1[attribute1] >= r2[attribute2]){
	  orEstimate /= (double) r1[attribute1];
	}
	else {
	  orEstimate /= (double) r2[attribute2];
	}

      } // end if curOp->left->code

	// Else we are parsing a single OR
      else {

        Operand *op = curOp->left;

	if (op->code != NAME){
	  op = curOp->right;
	}

	  // Get the relation name and the attribute
	ParseRelationAndAttribute(op, relation1, attribute1);
	r1 = relations[relation1];

	  // selection = algorithm: |R| / v(a)
	if (curOp->code == EQUALS){
	  orEstimate = (double) r1.GetNumRows() / (double) r1[attribute1];
	}

	  // selection < or > algorithm: |R| / 3
	else {
	  orEstimate = (double) r1.GetNumRows() / 3.00;
	}

      } // end else

    } // end else

    if (estimate == -1.0){
      estimate = orEstimate;
    }
    else {
      estimate *= orEstimate;
    }

    curAnd = curAnd->rightAnd;

  } // end while curAnd

  return estimate;

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

bool Statistics::CheckIndependence (struct OrList *parseTree){
	//Method checks to see if the ORs in this are Independent or not. Returns false if they are, true if they are not.
	//if all attributes in list are the same, it is not an independent or list
	//Independent:  Attributes in list are different.
	//Dependent: Attributes in list are the same
  struct OrList *curOr = parseTree;
  struct ComparisonOp *curOp;
  char *lName = NULL;
  vector<char *> checkVec;

  while (curOr){
	//When inside an OR, we want to start keeping track of what we see.  If we find an attribute that's not the same ,we need to mark that or list as dependent
	//not sure how Nick wants me to mark the specific or list, for now we just say that it's false and bail 
	prevSeen.clear(); //
	
	if(lName == NULL){
		lName = curOr->left->value;
		checkVec.push_back(lName);
	}
	else{
		if(curOr->left->value != checkVec[0]){
			lName = curOr->left->value;
			checkVec.push_back(lName);	
		}
	}

	curOr = curOr->rightOr;
      } // end while curOr

   return (checkVec.size() > 1); //Dependent will have checkVec size 1, Independent will have size > 1
}


bool Statistics::Exists(string relation) {
  return (relations.find(relation) != relations.end());
}
