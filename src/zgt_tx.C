/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

extern void *start_operation(long, long);  //starts opeartion by doing conditional wait
extern void *finish_operation(long);       // finishes abn operation by removing conditional wait
extern void *open_logfile_for_append();    //opens log file for writing
extern void *do_commit_abort(long, char);   //commit/abort based on char value (the code is same for us)

extern zgt_tm *ZGT_Sh;			// Transaction manager object

FILE *logfile; //declare globally to be used by all

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid){
  this->lockmode = (char)' ';  //default
  this->Txtype = type; //Fall 2014[jay] R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1; //set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; //init to  an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //Initialize a transaction object. Make sure it is
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. when creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
    struct param *node = (struct param*)arg;// get tid and count
    start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
    //tx->print_tm();
    open_logfile_for_append();
      #ifdef TX_DEBUG
        fprintf(logfile, "T%ld\t%c \tBeginTx\n", node->tid, node->Txtype);  // Write log record and close
        fflush(logfile);
      #endif 
    // Lock Tx manager; Add node to transaction list. Wait unitl it is done
    zgt_p(0); 
      while(get_tx(node->tid) == NULL){
        if(ZGT_Sh->lastr == NULL)
          ZGT_Sh->lastr = tx;
        else{
          tx->nextr = ZGT_Sh->lastr;
          ZGT_Sh->lastr = tx;
        }   
      }
      //tx->print_tm();
    zgt_v(0); 			// Release tx manager 
    finish_operation(node->tid);
    pthread_exit(NULL);				// thread exit

}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
    struct param *node = (struct param*)arg;// get tid and objno and count
    // struct param *node = (struct param*)arg;// get tid and count
    start_operation(node->tid, node->count);
    zgt_tx* tx = get_tx(node->tid);
    if(tx->status != TR_ABORT || tx->status != TR_END)
        int result = tx->set_lock(node->tid, tx->sgno, node->obno, node->count, 'S');
    else{
      #ifdef TX_DEBUG
        fprintf(logfile, "T%ld transaction already completed\n",tx->tid);  // Write log record and close
        fflush(logfile);
      #endif
    }
    finish_operation(node->tid);
    #ifdef TX_DEBUG
      printf("Read\n");
      fflush(stdout);
    #endif
    pthread_exit(NULL);       // thread exit
  //do the operations for reading. Write your code
}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
    struct param *node = (struct param*)arg;// get tid and objno and count
    // struct param *node = (struct param*)arg;// get tid and count
    start_operation(node->tid, node->count);
    zgt_tx* tx = get_tx(node->tid);
    if(tx->status != TR_ABORT || tx->status != TR_END)
        int result = tx->set_lock(node->tid, tx->sgno, node->obno, node->count, 'X');
    else{
      #ifdef TX_DEBUG
        fprintf(logfile, "T%ld transaction already completed\n",tx->tid);  // Write log record and close
        fflush(logfile);
      #endif
    }
    finish_operation(node->tid);
    #ifdef TX_DEBUG
      printf("Write\n");
      fflush(stdout);
    #endif
    pthread_exit(NULL);  
  //do the operations for writing; similar to readTx. Write your code

}

void *aborttx(void *arg)
{
  //write your code
  struct param *node = (struct param*)arg;// get tid and count  
  start_operation(node->tid, node->count);
  do_commit_abort(node->tid,'A');
  finish_operation(node->tid);
  #ifdef TX_DEBUG
    printf("Aborting\n");
    fflush(stdout);
  #endif
  fclose(logfile);
  // if(ZGT_Sh->lastr == NULL){
  //       //ZGT_Sh->endTm(0);
  //     }
  //fclose(logfile);
  pthread_exit(NULL);			// thread exit
}

void *committx(void *arg)
{
 //remove the locks before committing
  struct param *node = (struct param*)arg;// get tid and count
  //write your code
  start_operation(node->tid, node->count);
  do_commit_abort(node->tid,'C');
  finish_operation(node->tid);
  #ifdef TX_DEBUG
    printf("Committing\n");
    fflush(stdout);
  #endif
  // if(ZGT_Sh->lastr == NULL){
  //       //ZGT_Sh->endTm(0);
  //     }
  //fclose(logfile);
  pthread_exit(NULL);			// thread exit
}

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existant tx

void *do_commit_abort(long t, char status){
  // write your code
  #ifdef TX_DEBUG
      printf("Committing transaction %ld\n",t); // Write log record and close
      fflush(stdout);
  #endif
  zgt_tx* tx = get_tx(t);
  if(tx == NULL){
    #ifdef TX_DEBUG
      fprintf(logfile, "T%ld \t not present\n", t); // Write log record and close
      fflush(logfile);
    #endif
  }
  else{
    //free the locks and remove the transaction
    if(status == 'A'){
      tx->status = TR_ABORT;
      fprintf(logfile, "T%ld\t%c \tAbortTx  \t\t\t\t\t\t\t\t %c\n", tx->tid,tx->Txtype, tx->status); // Write log record and close
      fflush(logfile);
    }else if(status == 'C'){
      tx->status = TR_END;
      fprintf(logfile, "T%ld\t%c \tCommitTx \t\t\t\t\t\t\t\t %c\n", tx->tid,tx->Txtype, tx->status); // Write log record and close
      fflush(logfile);
    }
      //free the locks for both abort and commit operaitions and delete the trasaction from the Transaction manager 
      //list if it is a Commit operation.
      zgt_p(0);
         //tx->print_tm(); 
         tx->free_locks();
	       int count = zgt_nwait(tx->tid);
         #ifdef TX_DEBUG
           printf("Number of threads waiting on %ld:%d\n",t,count);
           //tx->print_tm();
           fflush(stdout);
        #endif
	      //Remove the locks on a trasaction by calling the sempahore V method for the current transaction
        //for(int i =0 ;i < count ; i++)
        if(count>0)  
	   zgt_v(tx->tid);
        if(status == 'C')
          tx->remove_tx();
      zgt_v(0);
      // for(zgt_tx *txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){  // scan through list
      //   printf("%ld \t", txptr->tid);
      // }
      //printf("\n");
      //zgt_p(0);
        //int count = zgt_nwait(tx->tid);
         //#ifdef TX_DEBUG
           //printf("Number of threads waiting on %ld:%d\n",t,count);
           //tx->print_tm();
           //fflush(stdout);
        //#endif
        //Remove the locks on a trasaction by calling the sempahore V method for the current transaction
        //if(count > 0)
          //zgt_v(tx->tid);
      //zgt_v(0);
  }
  //return 0;
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM(New Logic)
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  //check if the head transaction is the one to be removed.
  if(lastr1->tid == this->tid){
        printf("Removing transaction %ld\n",this->tid);
        ZGT_Sh->lastr = lastr1->nextr;
        return 0;
  }
  txptr = ZGT_Sh->lastr->nextr;
  while(txptr != NULL){
    if (txptr->tid == this->tid){
      printf("Removing transaction %ld\n",this->tid);
      lastr1->nextr = txptr->nextr;
      delete txptr;
      return (0);
    }
    lastr1 = txptr;
    txptr = txptr->nextr;
  }
  fprintf(logfile, "Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(logfile);
  printf("Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx in this*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0);
  zgt_tx* tx = get_tx(tid1);
  zgt_p(0);  
    zgt_hlink *linkp = ZGT_Ht->find(sgno1,obno1);    
    //Acquire a lock and perform the operaion if there is no lock
    //peresrnt on the object.
    if(linkp == NULL){
      tx->status = TR_ACTIVE;
      int res = ZGT_Ht->add(tx,sgno1,obno1,lockmode1);
      //ZGT_Ht->print_ht();
      if(res == -1){
        #ifdef TX_DEBUG
          printf("Out of memory\n");
          fflush(stdout);
        #endif
        zgt_v(0);
        return (-1);  
      }
      else{
        printf("Added lock for %ld\n",tx->tid);
        fflush(stdout);
      }
      zgt_v(0);
      tx->perform_readWrite(tid1,obno1, lockmode1);
      return (0);
    }
    //Check if the current transaction is the one holding the lock, if yes then proceed 
    //with the operation.
    else if(linkp->tid == tid1){
          zgt_v(0);
          tx->perform_readWrite(tid1,obno1, lockmode1);
          return (0);            
    }
  zgt_v(0);  

  //If lockmode is 'X' and if the object is held by another transaction
  //put the object in the wait status and make it wait for the previous transaction
  //to complete. If two or more transactions are waiting for the lock on same object
  //then the transactions that doesnt get the lock should again wait on the transaction
  //that is acquired the lock newly
  if(lockmode1 == 'X'){ 
      zgt_hlink *current = ZGT_Ht->find(sgno1,obno1);
      do{
        #ifdef TX_DEBUG
                printf("Inside wait %ld,%ld\n", tid1,current->tid);
                fflush(stdout);
        #endif
        tx->status = TR_WAIT;
        zgt_p(0);
	         setTx_semno(current->tid,current->tid);
        zgt_v(0);
        fprintf(logfile, "T%ld\t%c \tWriteTx \t %ld:%d:%d \t\tWriteLock \tWaiting \t W \n", tid1, tx->Txtype, obno1,ZGT_Sh->objarray[obno1]->value, ZGT_Sh->optime[tid]);
        fflush(logfile);
        zgt_p(current->tid); //wait here...
        zgt_p(0);
          if(ZGT_Ht->find(sgno1,obno1) == NULL){
            printf("Adding lock to Transaction %ld\n",tx->tid);
            ZGT_Ht->add(tx,sgno1,obno1,lockmode1);
            tx->status = TR_ACTIVE;
            current = ZGT_Ht->find(sgno1,obno1);
            if(current != NULL)
              printf("Current transaction%ld\n", current->tid);
            else
              printf("NULL \n");
            fflush(stdout);
          }
          else if(ZGT_Ht->findt(tx->tid,sgno1,obno1) != NULL && ZGT_Ht->findt(tx->tid,sgno1,obno1)->tid == tx->tid){
              current = ZGT_Ht->find(sgno1,obno1);
          }
          else{
            current = ZGT_Ht->find(sgno1,obno1);
            if(current != NULL)
              printf("Waiting again on %ld\n", current->tid);
            else
              printf("NULL \n");
          }
          ZGT_Ht->print_ht(); 
        zgt_v(0);
      }while(current->tid != tx->tid);
        tx->perform_readWrite(tid1,obno1, 'X'); 
       zgt_v(linkp->tid);
       #ifdef TX_DEBUG
           printf("after v operation \n");
           fflush(stdout);
       #endif
    return (0);
  }
  //If lockmode is 'S' and if the object is not being waited by other 
  //transactions then proceed with the operation, else make it wait on 
  //the other transcations. If the same transaction gets the lock then
  //proceed further by seettting a S lock on the object
  else{ 
        //logic to check the queue size of the transaction holding the
        //lock currently
        zgt_p(0);       
            if(zgt_nwait(linkp->tid) == 0){
              //ZGT_Ht->add(tx,sgno1,obno1,lockmode1);
              tx->status = TR_ACTIVE;
              zgt_v(0);
              tx->perform_readWrite(tid1,obno1, 'S');
              return(0);
            }
        zgt_v(0);
        zgt_hlink *current = linkp;
        do{
          // #ifdef TX_DEBUG
          //         printf("Inside Read wait %ld\n", tid1);
          //         fflush(stdout);
          // #endif
          tx->status = TR_WAIT;
          setTx_semno(current->tid,current->tid);
          fprintf(logfile, "T%ld\t%c \tReadTx \t %ld:%d:%d \t\tReadLock \tWaiting \t W \n", tid1, tx->Txtype, obno1,ZGT_Sh->objarray[obno1]->value, ZGT_Sh->optime[tid]);
          fflush(logfile);
          zgt_p(current->tid); //wait here...
          zgt_p(0);
            if(ZGT_Ht->find(sgno1,obno1) == NULL){
              printf("Adding lock to Transaction %ld\n",tx->tid);
              ZGT_Ht->add(tx,sgno1,obno1,lockmode1);
              tx->status = TR_ACTIVE;
              current = ZGT_Ht->find(sgno1,obno1);
              // if(current != NULL)
              //   printf("Current transaction%ld\n", current->tid);
              // else
              //   printf("NULL \n");
              // fflush(stdout);
            }
            else if(ZGT_Ht->findt(tx->tid,sgno1,obno1) != NULL && ZGT_Ht->findt(tx->tid,sgno1,obno1)->tid == tx->tid){
                current = ZGT_Ht->find(sgno1,obno1);
            }
            else{
              current = ZGT_Ht->find(sgno1,obno1);
              // if(current != NULL)
              //   printf("Waiting again on %ld\n", current->tid);
              // else
              //   printf("NULL \n");
            }
            ZGT_Ht->print_ht(); 
          zgt_v(0);
        }while(current->tid != tx->tid);
         zgt_v(linkp->tid);
      // #ifdef TX_DEBUG
      //     printf("after v operation \n");
      //     fflush(stdout);
      // #endif
      tx->perform_readWrite(tid1,obno1, 'X'); 
  }        
return (0);
}

// this part frees all locks owned by the transaction
// Need to free the thread in the waiting queue
// try to obtain the lock for the freed threads
// if the process itself is blocked, clear the wait and semaphores

int zgt_tx::free_locks()
{
  zgt_hlink* temp = head;  //first obj of tx
  
  //open_logfile_for_append();
   
  for(temp;temp != NULL;temp = temp->nextp){	// SCAN Tx obj list

      fprintf(logfile, "%ld : %d \t\n", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      fflush(logfile);
       if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1){
  	     printf(":::ERROR:node with tid:%ld and onjno:%ld was not found for deleting", this->tid, temp->obno);		// Release from hash table
  	     fflush(stdout);
        }
        else {
          #ifdef TX_DEBUG
  	       printf("\n:::Hash node with Tid:%ld, obno:%ld lockmode:%c removed\n",temp->tid, temp->obno, temp->lockmode);
  	       fflush(stdout);
          #endif
        }
    }
  //fprintf(logfile, "\n");
  //fflush(logfile);  
  return(0);
}		

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  //2014: not used
{
  zgt_tx *linktx, *prevp;

  linktx = prevp = ZGT_Sh->lastr;
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
  return 0;
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// check which other transaction has the lock on the same obno
// returns the hash node
zgt_hlink *zgt_tx::others_lock(zgt_hlink *hnodep, long sgno1, long obno1)
{
  zgt_hlink *ep;
  ep=ZGT_Ht->find(sgno1,obno1);
  while (ep)				// while ep is not null
    {
      if ((ep->obno == obno1)&&(ep->sgno ==sgno1)&&(ep->tid !=this->tid)) 
	return (ep);			// return the hashnode that holds the lock
      else  ep = ep->next;		
    }					
  return (NULL);			//  Return null otherwise 
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print

void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%ld\t%c\t%d\t%ld\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//currently not used
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}
void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the acutual read/write operation
// based  on the lockmode
void zgt_tx::perform_readWrite(long tid,long obno, char lockmode){
    zgt_tx* temp = get_tx(tid);
    if(lockmode == 'S'){
      ZGT_Sh->objarray[obno]->value = ZGT_Sh->objarray[obno]->value - 1;
      long value = ZGT_Sh->objarray[obno]->value;
      fprintf(logfile, "T%ld\t%c \tReadTx \t\t %ld:%ld:%d \t\tReadLock \tGranted \t P \n", temp->tid, temp->Txtype, obno, value,ZGT_Sh->optime[tid]); // Write log record and close
      fflush(logfile);
      sleep(ZGT_Sh->optime[tid]);
    }
    else if(lockmode == 'X'){
      ZGT_Sh->objarray[obno]->value = ZGT_Sh->objarray[obno]->value + 1;
      long value = ZGT_Sh->objarray[obno]->value;
      fprintf(logfile, "T%ld\t%c \tWriteTx \t %ld:%ld:%d \t\tWriteLock \tGranted \t P \n", temp->tid, temp->Txtype, obno, value,ZGT_Sh->optime[tid]); // Write log record and close
      fflush(logfile);
      sleep(ZGT_Sh->optime[tid]);
    }
}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %ld wants to wait on sem:%d of tid:%ld which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    return(-1);
  }
  if (txptr->semno == -1){
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%ld\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

// routine to start an operation by checking the previous operation of the same
// tx has completed; otherwise, it will do a conditional wait until the
// current thread signals a broadcast

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]); 
  //return 0;
}

// Otherside of the start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
  //return 0;
}

void *open_logfile_for_append(){
  
  if ((logfile = fopen(ZGT_Sh->logfile, "a")) == NULL){
    printf("\nCannot open log file for append:%s\n", ZGT_Sh->logfile);
    fflush(stdout);
    exit(1);
  }
  //return 0;
}


