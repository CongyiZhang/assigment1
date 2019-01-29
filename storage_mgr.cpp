//
// Created by zcy on 2019/1/24.
//
#include <iostream>
#include <fstream>
#include <math.h>
#include <io.h>
#include <stdio.h>
#include "storage_mgr.h"
using namespace std;

bool sysFlag {false};
SM_PageHandle buffer {nullptr};
SM_FileHandle fileHandle{};

void initStorageManager(){
    if (sysFlag){
        cout << "StorageManger already initialized";
        buffer = new char[PAGE_SIZE];
    } else {
        cout << "StorageManager is initialized";
        sysFlag = true;
    }
}

RC createPageFile (string *fileName){
    if (sysFlag){
        fstream inputFile {*fileName, ios::in|ios::app|ios::binary}; //input append binary
        if (!inputFile.good()) {
            cout << "Error creating file" << endl;
            return RC_WRITE_FAILED;
        } else {
            char newPage [PAGE_SIZE];
            for (auto temp:newPage){
                temp = '\0';
            }
//            newPage[0]='1';

            inputFile.read(newPage, sizeof(newPage));
            if (inputFile.gcount()!=PAGE_SIZE){
                inputFile.close();
                cout << "Error creating file" << endl;
                return RC_WRITE_FAILED;
            } else{
                inputFile.close();
                return RC_OK;
            }

        }
    } else {
        cout << "StorageManager is not initialized" << endl;
        return RC_WRITE_FAILED;
    }
}

RC openPageFile (string *fileName, SM_FileHandle *fileHandle) {
    if (sysFlag){
        fstream inputFile {*fileName, ios::in|ios::binary};
        if (!inputFile.good()){
            cout <<"Error opening file" << endl;
            return RC_FILE_NOT_FOUND;
        } else {
            inputFile.seekg(0, ios::cur);
            fileHandle->curPagePos = ceil(inputFile.tellg()/PAGE_SIZE-1);
            inputFile.seekg(0, ios::end);
            fileHandle->totalNumPages = ceil(inputFile.tellg()/PAGE_SIZE);
            fileHandle->fileName = fileName;
            fileHandle->mgmtInfo = &inputFile;
            return RC_OK;
        }

    } else {
        cout << "StorageManager is not initialized" << endl;
        return RC_FILE_NOT_FOUND;
    }
}

RC closePageFile (SM_FileHandle *fileHandle) {
    if (sysFlag){
        if ((*(fileHandle->mgmtInfo)).good()){
            (*(fileHandle->mgmtInfo)).close();
            delete fileHandle;
            return RC_OK;
        } else {
            return RC_FILE_NOT_FOUND;
        }
    } else {
        cout << "StorageManager is not initialized" << endl;
        return RC_FILE_NOT_FOUND;
    }
}

RC destroyPageFile (string *fileName) {
    if (sysFlag){
        fstream inputFile {*fileName, ios::binary};
        if (inputFile.good()){
            int flag = remove((*fileName).c_str());
            if (!flag){
                return RC_OK;
            } else {
                cout << "Error file removing" << endl;
                return RC_FILE_NOT_FOUND;
            }
        } else {
            cout <<"Error opening file" << endl;
            return RC_FILE_NOT_FOUND;
        }

    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_NOT_FOUND;
    }
}

/* reading blocks from disc */

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle menPage){
    if (sysFlag){
        if (fHandle->mgmtInfo == nullptr){
            return RC_FILE_NOT_FOUND;
        } else if (pageNum > fHandle->totalNumPages || pageNum < 0){
            return RC_READ_NON_EXISTING_PAGE;
        } else {
            (*fHandle->mgmtInfo).seekg(pageNum*PAGE_SIZE, ios::beg);
            (*fHandle->mgmtInfo).get(menPage, PAGE_SIZE);
            return RC_OK;
        }
    }else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

int getBlockPos(SM_FileHandle *fHandle){
    if (sysFlag){
        return fHandle->curPagePos;
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        return readBlock(0, fHandle, memPage);
    }
    else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        return readBlock(fHandle->curPagePos - 1, fHandle, memPage);

    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        return readBlock(fHandle->curPagePos, fHandle, memPage);
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        return readBlock(fHandle->totalNumPages-1, fHandle, memPage);
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        if(!(*fHandle->mgmtInfo).good()){
            cout <<"Error opening file" << endl;
            return RC_WRITE_FAILED;
        } else {
            if (pageNum >= fHandle->totalNumPages || pageNum < 0) {
                return RC_READ_NON_EXISTING_PAGE;
            } else {
                (*fHandle->mgmtInfo).seekp(pageNum*PAGE_SIZE, ios::end);
                (*fHandle->mgmtInfo).write(memPage, sizeof(*memPage));
                if ((*fHandle->mgmtInfo).gcount() < sizeof(char)*PAGE_SIZE){
                    return RC_WRITE_FAILED;
                } else {
                    fHandle->curPagePos = pageNum;
                    fHandle->totalNumPages ++;
                    return RC_OK;
                }
            }
        }
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (sysFlag){
        return writeBlock (fHandle->curPagePos, fHandle, memPage);
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
    if (sysFlag){
        if(!(*fHandle->mgmtInfo).good()){
            cout <<"Error opening file" << endl;
            return RC_WRITE_FAILED;
        } else {
            char * myPage = new char[PAGE_SIZE*sizeof(char)];
            myPage[0]='\0';
            fHandle->totalNumPages++;
            int rc = writeBlock (fHandle->totalNumPages-1, fHandle, myPage);
            delete [] myPage;
            myPage = nullptr;
            return rc;
        }
    } else {
        cout << "StorageManager is not initialized";
        return RC_FILE_HANDLE_NOT_INIT ;
    }
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    RC rc =RC_OK;
    if( access( fHandle->fileName, F_OK ) != -1 )
    {
        int i, existingNbOfPages, diff;
        existingNbOfPages=fHandle->totalNumPages;
        diff=numberOfPages-existingNbOfPages;
        if(diff>0)
        {
            for(i=0;i<diff;i++)
            {
                createPageFile(fHandle->fileName);
                fHandle->totalNumPages++;
                fHandle->curPagePos = fHandle->totalNumPages - 1;
            }

        }
    }
    else
    {
        rc = RC_FILE_NOT_FOUND;
    }
    return rc;
}