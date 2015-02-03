/*
 * A decision tree
 * Author: Hao Peng (penghao888@gmail.com)
 */

extern "C" {
#include<R.h>
#include<Rinternals.h>

#include"hpdtree.h"

#define DEBUG

/*
 * get the tree structure from an R vector of doubles
 * the first element is the number of nodes in the tree
 * the rest are tree node structures
 */
hpDecisionTree hpGetDecisionTree(SEXP Rtree)
{
   hpDecisionTree tree;
   double* t = REAL(Rtree);
   tree.nNodesP = (int *) t; 
   tree.maxNNodes = (length(Rtree)-1)*sizeof(double)/sizeof(hpDecisionTreeNode);
   tree.nodes = (hpDecisionTreeNode *) (t+1);
   return tree;
}
/*
 * Get serveral decistion tree structures from an R vector of doubles
 */
void hpGetDecisionTrees(SEXP Rtrees, int nTrees, 
                        // output variables
                        hpDecisionTree *trees)
{
   int treeLength = length(Rtrees)/nTrees;
   double* ts = REAL(Rtrees); 
   for (int iTree = 0; iTree < nTrees; iTree++)
   {  
      hpDecisionTree &tree = trees[iTree];
      double *t = ts + iTree * treeLength;
      tree.nNodesP = (int *) t;
      tree.maxNNodes = (treeLength-1)*sizeof(double)/sizeof(hpDecisionTreeNode);
      tree.nodes = (hpDecisionTreeNode*) (t+1);
   }
}
/*
 * create an R vector of doubles which stores a decision tree
 * the first element is the nubmer of nodes in the tree
 * followed by (maxNNodes) tree node structures
 */
SEXP hpNewDecisionTree(SEXP RmaxNNodes)
{
   SEXP Rtree;
   int maxNNodes = INTEGER(RmaxNNodes)[0];
   PROTECT(Rtree = allocVector(REALSXP, TREE_SIZE(maxNNodes)));
   double *t = REAL(Rtree);
   t[0] = 0;
   UNPROTECT(1);
   return Rtree;
}
/*
 * returne the size of a tree in double
 * used for creating decision tree in R
 */
SEXP hpGetDecisionTreeSize(SEXP RmaxNNodes)
{
   int maxNNodes = INTEGER(RmaxNNodes)[0];
   SEXP Rsize;
   PROTECT(Rsize = allocVector(INTSXP, 1));
   INTEGER(Rsize)[0] = TREE_SIZE(maxNNodes);
   UNPROTECT(1);
   return Rsize;
}

/*
 * initialize a node to be a terminal node
 */
void hpInitializeDecisionTreeNode(hpDecisionTreeNode &node,
                            double prediction)
{
   node.leftChild = 0;
   node.rightChild = 0;
   node.missingChild = 0;
   node.splitFeature = -1;
   node.splitValue = 0;
   node.prediction = prediction;
}
/*
 * initialize a decision tree with one root
 */
SEXP hpInitializeDecisionTree(hpDecisionTree &tree, double prediction)
{
   *(tree.nNodesP) = 1;
   hpInitializeDecisionTreeNode(tree.nodes[0], prediction);
   // return nothing
   return R_NilValue;
}
/*
 * a wrapper function for R to initialize a decision tree with one root
 */
SEXP hpInitDecisionTree(SEXP Rtree, SEXP Rprediction)
{
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   double prediction = REAL(Rprediction)[0];
   hpInitializeDecisionTree(tree, prediction);
   // return nothing
   return R_NilValue;
}
/*
 * split a nonterminal node and add three new terminals
 */
void hpSplitADecisionTreeNode(
               hpDecisionTree &tree,
               int iNode, // which node to split
               int splitFeature,
               double splitValue,
               double leftPrediction,
               double missingPrediction,
               double rightPrediction)
{
   #ifdef DEBUG
   if (iNode < 0 || iNode >= *(tree.nNodesP) || tree.nodes[iNode].leftChild)
      error("the node to split %d is invalid", iNode);
   #endif
   
   // add three new nodes
   *(tree.nNodesP) += 3;
   
   #ifdef DEBUG
   if (tree.maxNNodes < *(tree.nNodesP))
      error("the number of nodes %d for a tree exceeds the limit %d", *tree.nNodesP, tree.maxNNodes);
   #endif

   // turn current terminal node to a split node
   hpDecisionTreeNode &splitNode = tree.nodes[iNode];
   splitNode.prediction = 0;
   splitNode.splitFeature = splitFeature;
   splitNode.splitValue = splitValue;
   // the left child is placed at the third last of node list
   splitNode.leftChild = *(tree.nNodesP)-3;
   // the missing child is placed at the second last of node list
   splitNode.missingChild = *(tree.nNodesP)-2;
   // the right child is placed at the last of the node list
   splitNode.rightChild = *(tree.nNodesP)-1;

   // initialize new terminal nodes
   hpInitializeDecisionTreeNode(tree.nodes[splitNode.leftChild],leftPrediction);
   hpInitializeDecisionTreeNode(tree.nodes[splitNode.missingChild],missingPrediction);
   hpInitializeDecisionTreeNode(tree.nodes[splitNode.rightChild],rightPrediction);
}
/*
 * compute the node to terminal mapping
 * by assigning a node to a terminal according to the order of node list
 * Warning: the resulting nodeToTerminal can be inconsistant with other methods
 */
void hpComputeNodeToTerminal(hpDecisionTree &tree, int* nodeToTerminal)
{
   int iTerminal = 0;
   for (int iNode = 0; iNode < *tree.nNodesP; iNode++)
      if (IS_TERMINAL_NODE(tree,iNode)) // terminal node
         nodeToTerminal[iNode] = iTerminal++;
}
/*
 * print a decision tree
 */
SEXP hpPrintDecisionTree(SEXP Rtree)
{
   hpDecisionTree tree = hpGetDecisionTree(Rtree);
   Rprintf("%4s %10s %10s %10s %10s %10s %10s\n", "Node", "Left", "Missing", "Right", "Prediction","Feature", "Value");
   for (int iNode = 0; iNode < *(tree.nNodesP); iNode++)
   {
      hpDecisionTreeNode &node = tree.nodes[iNode];
      Rprintf("%3d: %10d %10d %10d %10lf %10d %10lf\n",
         iNode, node.leftChild, node.missingChild, node.rightChild,
         node.prediction, node.splitFeature, node.splitValue);
   }
   // return nothing
   return R_NilValue;
}

SEXP hpConvertTreeFormat(SEXP inTree)
{
  SEXP outTree;
  hpDecisionTree tree = hpGetDecisionTree(inTree);
  PROTECT(outTree = allocVector(REALSXP, 7*(*tree.nNodesP)));
  double *t = REAL(outTree);
  memset(t, 0, 7*tree.maxNNodes*sizeof(double));
  hpConvertNodeFormat(&tree,t,0);
  UNPROTECT(1);

  return outTree;
}
  
double* hpConvertNodeFormat(hpDecisionTree* tree, double* outNode, int nodeID)
{
  hpDecisionTreeNode inNode = tree->nodes[nodeID];
  outNode[0] = nodeID+1;
  outNode[1] = inNode.splitFeature;
  outNode[2] = inNode.splitValue;
  outNode[3] = inNode.prediction;
  outNode[4] = inNode.leftChild? inNode.leftChild+1:-1;
  outNode[5] = inNode.rightChild? inNode.rightChild+1:-1;
  outNode[6] = inNode.missingChild?inNode.missingChild+1:-1;
  if(inNode.leftChild)
    outNode = hpConvertNodeFormat(tree, outNode + 7, inNode.leftChild);
  if(inNode.rightChild)
    outNode = hpConvertNodeFormat(tree, outNode + 7, inNode.rightChild);
  if(inNode.missingChild)
    outNode = hpConvertNodeFormat(tree, outNode + 7, inNode.missingChild);

  return outNode;
} 

}

