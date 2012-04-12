#ifndef STATISTICS_H
#define STATISTICS_H

#include "ParseTree.h"
#include "RelationStatistics.h"

#include <map>
#include <set>
#include <vector>
#include <iostream>

using std::map;
using std::set;
using std::string;
using std::vector;
using std::ostream;

class Statistics
{

public:

	Statistics ();
	Statistics (Statistics &copyMe);	 // Performs deep copy
	~Statistics ();

	Statistics& operator= (Statistics &stat);
	int operator() (char *relation);
	int operator() (char *relation, char *attribute);

	void AddRel (char *relName, int numTuples);
	void AddAtt (char *relName, char *attName, int numDistincts);
	void CopyRel (char *oldName, char *newName);
	
	void Read (char *fromWhere);
	void Write (char *fromWhere);

	void  Apply (struct AndList *parseTree,
		     char *relNames[], int numToJoin);

	double Estimate (struct AndList *parseTree,
			char **relNames, int numToJoin);


private:

	map<string,RelationStats> relations;
	vector<RelationSet> sets;

	map<string,string> tables;

	bool Exists(string relation);

	double Estimate (struct AndList *parseTree,
			RelationSet toEstimate, vector<int> &indexes);

	bool CheckParseTree (struct AndList *parseTree);
	bool CheckSets (RelationSet toEstimate, vector<int> &indexes);
	double GenerateEstimate (struct AndList *parseTree);

	void ParseRelationAndAttribute(struct Operand *op, string &relation,
				       string &attribute);

	bool CheckIndependence (struct OrList *parseTree); //Checks for the presence of independent ors
};

#endif
