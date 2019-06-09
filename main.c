#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>

/* wątki */
#include <pthread.h>

/* stałe dot zasobów */
#define SEA_SLOTS 20 // maksymalna liczba turystów na morzu
#define VEHICLE_SLOTS 10 // liczba pojazdów
#define TECHNICIAN_SLOTS 4 // liczba techników
#define VEHICLE_STARTING_HEALTH 10 // maksymalna wartość stanu technicznego pojazdu
#define VEHICLE_HEALTH_MAX_DECLINE 3 // maksymalna wartość ubytku stanu technicznego pojazdu po jednej wycieczce

/* boolean */
#define TRUE 1
#define FALSE 0

/* typy wiadomości */
#define FINISH 1
#define APP_MSG 2
#define MSG_TYPE_REQ 1
#define MSG_TYPE_REL 2
#define MSG_TYPE_OK 3

/* możliwe stany */
#define STATE_AWAIT_TOURISTS 1 // oczekiwanie na osoby
#define STATE_AWAIT_SEA_ACCESS 2 // oczekiwanie na dostęp do morza
#define STATE_AWAIT_VEHICLE_ACCESS 3 // oczekiwanie na pojazd
#define STATE_TOUR_IN_PROGRESS 4 // wycieczka trwa
#define STATE_AWAIT_TECHNICIAN 5 // oczekiwanie na technika
#define STATE_REPAIR_IN_PROGRESS 6 // oczekiwanie na zakończenie naprawy przez technika

/* możliwe typy zasobów */
#define RESOURCE_SEA 1 // morze
#define RESOURCE_VEHICLE 2 // pojazd
#define RESOURCE_TECHNICIAN 3 // technik

char passive = FALSE;

/* TYPY WIADOMOŚCI */

// wiadomość REQ
typedef struct {
    int resource_type; // typ zasobu
    int resource_id; // id zasobu (w przypadku rozróżnialnego - pojazdu)
    int timestamp; // znacznika zeg. Lamporta
} msg_REQ;

// wiadomość REL
typedef struct {
    int resource_type; // typ zasobu
    int resource_id; // id zasobu (w przypadku rozróżnialnego - pojazdu)
    int resource_update_value; // zaktualizowana wartość zasobu (zużycie pojazdu)
    int timestamp; // znacznika zeg. Lamporta
} msg_REL;

// wiadomość OK
typedef struct {
    int timestamp; // znacznika zeg. Lamporta
} msg_OK;

/* FUNKCJE WYSYŁAJĄCE WIADOMOŚCI */

void sendREQ(int resource_type, int resource_id) {

    msg_REQ msg;
    msg.resource_id = resource_id;
    msg.resource_type = resource_type;
    msg.timestamp = ++l_clock;

    MPI_Send(&msg, sizeof(msg), MPI_INT, target, MSG_TYPE_REQ, MPI_COMM_WORLD);
    // TODO - poprawić aby rozsyłać do każdego
}

void sendREL(int resource_type, int resource_id, int resource_update_value) {

    msg_REL msg;
    msg.resource_id = resource_id;
    msg.resource_type = resource_type;
    msg.resource_update_value = resource_update_value;
    msg.timestamp = ++l_clock;

    MPI_Send(&msg, sizeof(msg), MPI_INT, target, MSG_TYPE_REL, MPI_COMM_WORLD);
    // TODO - poprawić aby rozsyłać do każdego
}

void sendOK(int target) {

    msg_OK msg;
    msg.timestamp = ++l_clock;

    MPI_Send(&msg, sizeof(msg), MPI_INT, target, MSG_TYPE_OK, MPI_COMM_WORLD);
}

/* REPREZENTACJA STANU PRZEWODNIKA */

// mutex chroniący dostęp do stanu przewodnika
pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;

// obecny stan
int current_state = STATE_AWAIT_TOURISTS;

// zegar Lamporta
int l_clock = 0;

// liczba odebranych OK
int ok_count = 0;

// kolejka do morza
// TODO

// tablica kolejek do pojazdów
// TODO

// kolejka do techników
// TODO

// tablica stanów pojazdów
// TODO

// lista numerów pojazdów w kolejce do których stoimy
// TODO

/* TWORZONE WĄTKI */

// wątek oczekujący na turystów
void *awaitTouristsThread(void *ptr)
{
    // losowy sleep

    // wylosuj liczbę użytkowników

    // mutex on

    // wyślij REQ do morza

    // zmień stan na 2 (czekamy na dostęp do morza)

    // mutex off
}

// wątek oczekujący na zakończenie wycieczki
void *awaitTourEndThread(void *ptr)
{
    // losowy sleep

    // mutex on

    // wylosuj spadek stanu technicznego pojazdu

    // jeśli nowy stan techniczny > 0
    // wyślij REL dla pojazdu
    // wyślij REL dla morza
    // przejdź do stanu 1 - oczekuj na turystów

    // wpp - jeśli pojazd się popsuł
    // wyślij REQ dla technika
    // przejdź do stanu 5 - oczekuj na technika

    // mutex off
}

// wątek oczekujący na zakończenie naprawy
void *awaitVehicleRepairThread(void *ptr)
{
    // losowy sleep

    // mutex on

    // REL dla technika
    
    // REL dla pojazdu z MAX stanem technicznym

    // przejdź do stanu 1 - oczekuj na turystów

    // mutex off
}

// TODO - funkcja logująca (w konsoli)

void log(int id, int timestamp, char *message) {
    // TODO
}

// TODO - funkcje odpowiedzialne za zarządzanie kolejkami

// updateRel, updateReq, findShortestVehicleQueue
// canAccessTechnician, canAccessSea, canAccessVehicle


// główna funkcja
int main(int argc, char **argv)
{
    printf("poczatek\n");
    int provided;

    MPI_Init_thread(&argc, &argv,MPI_THREAD_MULTIPLE, &provided);

    printf("THREAD SUPPORT: %d\n", provided);
    switch (provided) {
        case MPI_THREAD_SINGLE: 
            printf("Brak wsparcia dla wątków, kończę\n");
            /* Nie ma co, trzeba wychodzić */
	    fprintf(stderr, "Brak wystarczającego wsparcia dla wątków - wychodzę!\n");
	    MPI_Finalize();
	    exit(-1);
	    break;
        case MPI_THREAD_FUNNELED: 
            printf("tylko te wątki, ktore wykonaly mpi_init_thread mogą wykonać wołania do biblioteki mpi\n");
	    break;
        case MPI_THREAD_SERIALIZED: 
            /* Potrzebne zamki wokół wywołań biblioteki MPI */
            printf("tylko jeden watek naraz może wykonać wołania do biblioteki MPI\n");
	    break;
        case MPI_THREAD_MULTIPLE: printf("Pełne wsparcie dla wątków\n");
	    break;
        default: printf("Nikt nic nie wie\n");
    }

    // inicjalizacja 
    int size, rank;
    MPI_Status status;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // tworzymy wątek oczekujący na zgłoszenie się turystów
    pthread_t threadStart;
    pthread_create( &threadStart, NULL, awaitTouristsThread, 0);

    // główna pętla

    while(true) {

        // czekamy na wiadomości
        // TODO - nie pamiętam co zrobić żeby czekać na wiadomości dowolnego typu
        // i potem castować na konkretny typ
        msg_REQ msg;
        MPI_Recv( &msg, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        int msg_type = 0; // TODO
        int sender = 0; // TODO

        // zakładamy mutex
        pthread_mutex_lock(&mut);

        // aktualizacja zeg. Lamporta
        if (msg.timestamp > l_clock) {
            l_clock = msg.timestamp + 1;
        } else {
            l_clock++;
        }
        
        // maszyna stanów
        switch(current_state) {
            case STATE_AWAIT_TOURISTS:
                // czekamy na turystów - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK)
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                if (msg_type == MSG_TYPE_REL)
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value);
                if (msgtype == MSG_TYPE_REQ)
                    updateReq(msg.resource_type, msg.resource_id);
                    sendOK(sender);
                break;

            case STATE_AWAIT_SEA_ACCESS:
                // oczekujemy na dostęp do morza
                // tutaj chcemy zliczać liczbę OKejek które dostajemy
                // i jeśli jest ona dostatecznie wysoka - wchodzimy
                if (msg_type == MSG_TYPE_OK) {
                    ok_count++;
                    if (ok_count == size) { //już wszystkie procesy zaktualizowały informacje
                        if (canAccessSea(rank)) {
                            current_state = STATE_AWAIT_VEHICLE_ACCESS;
                            ok_count = 0;
                            int best_vehicle = findShortestVehicleQueue();
                            sendREQ(RESOURCE_VEHICLE, best_vehicle);
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value);
                    if (msg.resource_type == RESOURCE_SEA) { //REL dotyczył dostępu do morza
                        if (ok_count == size) { //i wszystkie inne procesy już wiedzą że też chcemy
                            if (canAccessSea(rank)) {
                                current_state = STATE_AWAIT_VEHICLE_ACCESS;
                                ok_count = 0;
                                int best_vehicle = findShortestVehicleQueue();
                                sendREQ(RESOURCE_VEHICLE, best_vehicle);
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id);
                    sendOK(sender);
                }
                break;

            case STATE_AWAIT_VEHICLE_ACCESS:
                // jeśli wiadomość to OK to zwiększamy OK
                // jeśli dostaliśmy OK od wszystkich, to sprawdzamy czy można wejść
                //     jeśli nie można to ustawiamy się w kolejce do jeszcze innego
                //     jeśli można to wchodzimy (wysyłając rel do każdego innego)
                // jeśli wiadomośc to REL to aktualizujemy kolejkę
                //     i jeśli otrzymaliśmy komplet OK to sprawdzamy czy możemy wejść (jeśli rel dotyczy pojazdu w kolejce do którego stoimy)
                // jeśli wiadomość to REQ to aktualizujemy odpowiednią kolejkę
                // spore TODO
                break;

            case STATE_TOUR_IN_PROGRESS:
                // czekamy na koniec wycieczki - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK)
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                if (msg_type == MSG_TYPE_REL)
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value);
                if (msgtype == MSG_TYPE_REQ)
                    updateReq(msg.resource_type, msg.resource_id);
                    sendOK(sender);
                break;
            
            case STATE_AWAIT_TECHNICIAN:
                if (msg_type == MSG_TYPE_OK) {
                    ok_count++;
                    if (ok_count == size) { //już wszystkie procesy zaktualizowały informacje
                        if (canAccessTechnician(rank)) {
                            // jeśli mogę to korzystam z usługi technika
                            current_state = STATE_REPAIR_IN_PROGRESS;
                            ok_count = 0;
                            // tworzymy nowy wątek oczekiwania na koniec naprawy
                            pthread_t threadA;
                            pthread_create( &threadA, NULL, awaitVehicleRepairThread, 0);
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REL) {
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value);
                    if (msg.resource_type == RESOURCE_SEA) { //REL dotyczył dostępu do morza
                        if (ok_count == size) { //i wszystkie inne procesy już wiedzą że też chcemy
                            if (canAccessTechnician(rank)) {
                                current_state = STATE_REPAIR_IN_PROGRESS;
                                ok_count = 0;
                                // tworzymy nowy wątek oczekiwania na koniec naprawy
                                pthread_t threadA;
                                pthread_create( &threadA, NULL, awaitVehicleRepairThread, 0);
                            }
                        }
                    }
                }
                if (msg_type == MSG_TYPE_REQ) {
                    updateReq(msg.resource_type, msg.resource_id);
                    sendOK(sender);
                }
                break;

            case STATE_REPAIR_IN_PROGRESS:
                // czekamy na koniec naprawy - nic szczególnego się nie dzieje
                if (msg_type == MSG_TYPE_OK)
                    ok_count++; // w teorii to nigdy nie powinno wykonać się w tym stanie
                if (msg_type == MSG_TYPE_REL)
                    updateRel(msg.resource_type, msg.resource_id, msg.resource_update_value);
                if (msgtype == MSG_TYPE_REQ)
                    updateReq(msg.resource_type, msg.resource_id);
                    sendOK(sender);
                break;
        }       

        // zdejmujemy mutex
        pthread_mutex_unlock(&mut);
        // i wracamy do początku pętli oczekując na kolejne wiadomości

    }

    // TODO - ubrać poniższe w przechwytywanie sygnału Ctrl+C (?) -- nie mam pewności
    pthread_mutex_destroy(&mut);

    MPI_Finalize();
}