#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <iostream>
#include <string.h>
#include <fstream>
#include <set>
#include <math.h>
#include <vector>
#include <string>

#define MAXFILES 3001
#define MAXFILENAME 100

//make a struct to pass to the thread
struct thread_data{
    int thread_id;
    std::vector<std::string> files;
    int countFiles;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    int countReducers;
    std::vector<std::set<long int>> *mapperSet;
    int *index;
};

//make a struct to pass to the thread
struct thread_data2{
    //power = id + 2
    int thread_id;
    int countReducers;
    int countMappers;
    pthread_barrier_t *barrier;
    std::vector<std::vector<std::set<long int>>> *mapperSet;
};

int isInteger(double x) {

    //check if a number is close to an integer with an error of 0.0001
    if (fabs(x - round(x)) < 0.0001) {
        return 1;
    }
    return 0;
}

int getNthRoot(long int x, int n) {
        
        
    int exponent = n;
    int lo = 2;
    int hi = lo;

    if(x == 1) {
        return 1;
    }
        
    while (pow(hi, exponent) <= x) {
        hi *= 2;
    }
        
    while (hi - lo > 1) {
        int middle = (lo + hi) / 2;
        
        if (pow(middle, exponent) <= x) {
            lo = middle;
        } else {
            hi = middle;
        }
    }
        
    if (pow(lo, exponent) - x == 0) {
        return 1;
    }
    return 0;
}




void* fmapper(void *arg) {

    long int x;
    int count;
    std::ifstream file;
    thread_data *data = (thread_data *) arg;

    //open files while there are still files to be read
    while(*data->index < data->countFiles) {
        //if a file is opened by a thread, lock its index
        pthread_mutex_lock(data->mutex);
        int index = *data->index;
        *data->index = *data->index + 1;
        //unlock the other thread that will work with another index file
        pthread_mutex_unlock(data->mutex);

        //open the file and read the values
        file.open(data->files[index]);
        file >> count;

        for(int j = 0; j < count; j++) {
            file >> x;
            //check for every exponent which numbers are perfect powers
            for(int exp = 2; exp < data->countReducers + 2; exp++) {
                if(getNthRoot(x, exp) == 1 ) {
                    data->mapperSet->at(exp - 2).insert(x);
                }
            }
        }

        file.close(); 
    }

    //this barrier is made as a warning for the other threads that this 
    //specific mapper has finished its work
    pthread_barrier_wait(data->barrier);
    pthread_exit(NULL);
}

void* freducer(void *arg) {
    //this barrier will stop the reducers from their work until the mappers
    //finish their code
    thread_data2 *data = (thread_data2 *) arg;
    pthread_barrier_wait(data->barrier);

    std::ofstream output;
    std::string filename = "out" + std::to_string(data->thread_id + 2) + ".txt";
    output.open(filename);
    std::set<int> result;
    
    //from every mapper extract their specific "exp" set 
    //and merge them into a final set
    for(int i = 0; i < data->countMappers; i++) {
        for(auto& e: data->mapperSet->at(i).at(data->thread_id)) {
            result.insert(e);
        }
    }

    //output is the size of the set
    output << result.size();

    output.close();
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {

    int countMappers;
    int countReducers;
    int countFiles;
    char folderName[50];
    std::vector<std::string> files;
    std::vector<thread_data> mapperData;
    pthread_mutex_t mutex;
    std::vector<std::vector<std::set<long int>>> arrayMapperSet;
    std::vector<thread_data2> reducerData;
    pthread_barrier_t barrier;
    int index;
    pthread_mutex_init(&mutex, NULL);

    index = 0;

    if (argc != 4) {
        printf("Usage: tema <countMappers> <countReducers> <folder>\n");
        return 0;
    }

    countMappers = atoi(argv[1]);
    countReducers = atoi(argv[2]);
    strcpy(folderName, argv[3]);

    //open the folder
    std::ifstream folder;
    folder.open(folderName);

    //memory for the array of array of sets
    arrayMapperSet.resize(countMappers);
    for(int i = 0; i < countMappers; i++) {
        arrayMapperSet[i].resize(countReducers);
    }

    //memory for the array of files
    files.resize(MAXFILES);
    for(int i = 0; i < MAXFILES; i++) {
        files[i].resize(MAXFILENAME);
    }

    //memory for the array of thread_data
    mapperData.resize(countMappers);

    //memory for the array of thread_data2
    reducerData.resize(countReducers);

    //read the files from the folder
    folder >> countFiles;
    std::string filename;
    for(int i  = 0; i < countFiles; i++) {
        folder >> filename;
        files[i].assign(filename);
    }

    //create a barrier that waits for both mappers and reducers to finish
    pthread_barrier_init(&barrier, NULL, countMappers + countReducers);

    //create the mappers
    pthread_t *mappers = (pthread_t *) malloc(countMappers * sizeof(pthread_t));

    //create the reducers
    pthread_t *reducers = (pthread_t *) malloc(countReducers * sizeof(pthread_t));

    for (int i = 0; i < countMappers + countReducers; i++) {
        if(i < countMappers) {
            mapperData[i].thread_id = i;
            mapperData[i].files = files;
            mapperData[i].countFiles = countFiles;
            mapperData[i].mutex = &mutex;
            mapperData[i].barrier = &barrier;
            mapperData[i].countReducers = countReducers;
            mapperData[i].mapperSet = &arrayMapperSet[i];
            mapperData[i].index = &index;
            pthread_create(&mappers[i], NULL, &fmapper, (void *) &mapperData[i]);
        } else {
            reducerData[i - countMappers].thread_id = i - countMappers;
            reducerData[i - countMappers].countMappers = countMappers;
            reducerData[i - countMappers].barrier = &barrier;
            reducerData[i - countMappers].mapperSet = &arrayMapperSet;
            pthread_create(&reducers[i - countMappers], NULL, &freducer, (void *) &reducerData[i - countMappers]);
        }
    }

    //join the mappers
    for (int i = 0; i < countMappers + countReducers; i++) {
        if(i < countMappers) {
            pthread_join(mappers[i], NULL);
        } else {
            pthread_join(reducers[i - countMappers], NULL);
        }
    }

    //close the folder
    folder.close();

    //destroy the barrier
    pthread_barrier_destroy(&barrier);

    //destroy the mutex
    pthread_mutex_destroy(&mutex);

    return 0;
}