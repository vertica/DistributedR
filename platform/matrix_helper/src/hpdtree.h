/*
 * Author: Hao Peng (penghao888@gmail.com)
 */
#ifndef _hpdtree_h
#define _hpdtree_h

#define IS_TERMINAL_NODE(tree, iNode) (tree.nodes[iNode].leftChild == 0)
#define TREE_SIZE(n) (1+(sizeof(hpDecisionTreeNode)*n+sizeof(double)-1)/sizeof(double))

/*
 * a tree node for a decision tree
 * if either leftChild or rightChild is 0, it is a terminal node
 */
struct __attribute__((__packed__)) hpDecisionTreeNode
{
   int leftChild;
   int rightChild;
   int missingChild;
   int splitFeature;
   double splitValue;
   double prediction;
};

/*
 * a structure of a decision tree
 */
struct hpDecisionTree {
   int* nNodesP;
   int maxNNodes;
   hpDecisionTreeNode *nodes;
};


hpDecisionTree hpGetDecisionTree(SEXP Rtree);

void hpGetDecisionTrees(SEXP Rtrees, int nTrees, hpDecisionTree *trees);

SEXP hpNewDecisionTree(SEXP RmaxNNodes);

void hpInitializeDecisionTreeNode(hpDecisionTreeNode &node, double prediction);

SEXP hpInitializeDecisionTree(hpDecisionTree &tree, double prediction);

SEXP hpInitDecisionTree(SEXP Rtree, SEXP Rprediction);

void hpSplitADecisionTreeNode(
               hpDecisionTree &tree,
               int iNode, 
               int splitFeature, 
               double splitValue,
               double leftPrediction,
               double missingPrediction,
               double rightPrediction);

void hpComputeNodeToTerminal(hpDecisionTree &tree, int* nodeToTerminal);

SEXP hpPrintDecisionTree(SEXP Rtree);

SEXP hpConvertTreeFormat(SEXP inTree);

double* hpConvertNodeFormat(hpDecisionTree* tree, double* outNode, int nodeID);

#endif
