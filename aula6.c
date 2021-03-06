// para compilar: gcc medianacol.c -o medianacol -Wall
// para executar: medianacol

/*
Dada uma matriz M de tamanho LxC, composta por números inteiros, construa um algoritmo em C que insira a mediana 
dos valores de cada coluna da matriz M em um vetor V de tamanho C e imprima esse vetor de medianas como saída 
dessa aplicação. 
As dimensoes de M e os números de M serão carregados de um arquivo texto no início da execução.
A primeira linha do arquivo de entrada deve indicar as dimensoes L e C, respectivamente.
As L demais linhas do arquivo indicam os C elementos de cada linha de M.

Considere este exemplo com uma matriz M de 6x4.
Os dados lidos do arquivo são:
6	4
9	3	7	5
4	12	2	40
8	8	4	32
6	14	32	21
33	44	20	1
10	18	17	10

As colunas ordenadas da matriz são (dados temporários):
4	3	2	1
6	8	4	5
8	12	7	10
9	14	17	21
10	18	20	32
33	44	32	40

Saída da aplicação salva em saida.txt (medianas das colunas):
8.5	13.0	12.0	15.5

*/


// Based on Author: Paulo S L Souza 
// quick sort code adapted from https://www.geeksforgeeks.org/quick-sort/


#include<stdio.h>
#include<stdlib.h>
#include <mpi.h>


void ordena_colunas(int *, int, int);
void calcula_mediana(int *, float *, int, int);
void quicksort(int *, int, int, int);
int partition (int *, int, int, int);
int master();
void slave(int my_rank);



/* low  --> Starting index,  high  --> Ending index */
//https://www.geeksforgeeks.org/quick-sort/
void quicksort(int *arr, int low, int high, int C)
{
    int pi;
    
    if (low < high)  {
        /* pi is partitioning index, arr[pi] is now
           at right place */
        pi = partition(arr, low, high, C);

        quicksort(arr, low, pi - 1, C);  // Before pi
        quicksort(arr, pi + 1, high, C); // After pi
    }
    
} // fim quicksort

/* This function takes last element as pivot, places
   the pivot element at its correct position in sorted
    array, and places all smaller (smaller than pivot)
   to left of pivot and all greater elements to right
   of pivot 
   https://www.geeksforgeeks.org/quick-sort/
*/
int partition (int *arr, int low, int high, int C)
{
    int i, j, swap, pivot;
    
    // pivot (Element to be placed at right position)
    pivot = arr[high*C];  
 
    i = (low - 1);  // Index of smaller element

    for (j = low; j <= high-1; j++)
    {
        // If current element is smaller than or
        // equal to pivot
        if (arr[j*C] <= pivot)
        {
            i++;    // increment index of smaller element
            
            // swap arr[i] and arr[j]
            swap = arr[i*C];
	    arr[i*C] = arr[j*C];
	    arr[j*C] = swap;
        }
    }
    
    //swap arr[i + 1] and arr[high]
    swap = arr[(i + 1)*C];
    arr[(i + 1)*C] = arr[high*C];
    arr[high*C] = swap;
    
    return (i + 1);
  
} // fim partition



void ordena_colunas(int *M, int L, int C) 
{
  int j;
  
  for (j = 0; j < C; j++) {
      //manda o endereco do primeiro elemento da coluna, limites inf e sup e a largura da matriz
      quicksort(&M[j], 0, L-1, C);
  }
} // fim ordena_colunas


void calcula_mediana(int *M, float *vet, int L, int C) 
{  
  int j;
  
//  printf("calcula_mediana: L=%d e C=%d \n", L, C);
  
  for (j = 0; j < C; j++) {
    vet[j] = M[((L/2)*C)+j];
//    printf("vet[%d] = %f, pos = %d, M[]=%d, ", j, vet[j], (L/2)*C+j, M[((L/2)*C)+j]);
    // se o nr de elementos eh par, tem que fazer a media
    // das duas medianas. Tem que pegar a mediana anteior.
    if(!(L%2))  {
      vet[j]+=M[((((L-1)/2)*C)+j)];
//    printf("vet[%d] = %f, pos = %d, M[]=%d, \n", j, vet[j], (((L-1)/2)*C)+j, M[((((L-1)/2)*C)+j)]);   
      vet[j]*=0.5;
    } // fim do if 
  } // fim do for
  
  return;
} // fim calcula_mediana



int main(int argc, char **argv)
{

/*
    // impressao para verificacao apenas
    for(i=0; i<L; i++)
    {
      for(j=0; j<C; j++)
	  printf("%d	", M[i*C+j]);
      printf("\n");
    }
*/
    // ordena as colunas de M usando o quicksort
    //ordena_colunas(M, L, C);

/*    printf("Matriz com colunas ordenadas:\n");
    // impressao para verificacao apenas
    for(i=0; i<L; i++)
    {
      for(j=0; j<C; j++)
	  printf("%d	", M[i*C+j]);
      printf("\n");
    }
*/
    //calcula_mediana(M, vet, L, C);

  //Inicializa MPI
   int i,j;
  int my_rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);


  if(my_rank == 0) {
    //printf("processo raiz rank %d\n", my_rank);

    //int fim_primeiro = C/4;
    //int fim_segundo = C/2;
    //int fim_terceiro = (C/4)*3;
    //int fim_quarto = C-1;
    
    master();

  } else {
    //printf("processo slave rank %d\n", my_rank);
    slave(my_rank);
  }


  MPI_Finalize();
  
  return(0);
    
} // fim da main


int master() {  
  int L, C;
  int *M;
  float *vet;
  int i, j;

  FILE *arquivo_entrada,*arquivo_saida;

  if(!(arquivo_entrada=fopen("entrada.txt","r"))) 
  {
    printf("Erro ao abrir entrada.txt como leitura! Saindo! \n");
    return(-1);
  }

  if(!(arquivo_saida=fopen("saida.txt","w+")))  
  {
    printf("Erro ao abrir/criar saida.txt como escrita. Saindo! \n");
    return(-1);
  }

  // Leitura da primeira linha de entrada.txt contendo as dimensoes de M
  fscanf(arquivo_entrada, "%d %d", &L, &C);
  //printf("Ordem da Matriz M: L=%d C=%d\n", L, C);

  // criando M[LxC]
  M = (int *) malloc ( L * C * sizeof(int));

  // criando vet[C] do tipo float. Este vetor terá as medianas das colunas.
  vet = (float *) malloc (C * sizeof (float) );

  // carregando M do arquivo
  for(i=0; i<L; i++) {
    for(j=0; j<C; j++) {
     fscanf(arquivo_entrada, "%d", &M[j*L+i]);
    }
  }

  /*for(i=0; i<L*C; i++) {
    printf("matriz[%d]=%d\n", i, M3[i]);
  }*/

  int npes; 
  MPI_Comm_size(MPI_COMM_WORLD, &npes);

  int send_buf[2];


  send_buf[0] = L;
  send_buf[1] = C/4;
  //printf("master: num linhas %d num colunas %d\n", send_buf[0], send_buf[1]);

  for(i=1; i < 3; i++) {//manda linha e coluna pra todos os slaves-1
      MPI_Send((void*)send_buf, 2, MPI_INT, i, 0, MPI_COMM_WORLD);
  }
  send_buf[1] += C%4;
  //printf("ultimo slave %d vai receber %d\n", i, send_buf[1]);
  MPI_Send((void*)send_buf, 2, MPI_INT, i, 0, MPI_COMM_WORLD);//manda pro ultimo slave todas as colunas que sobraram

  int primeiro = C/4;
  for(i=1; i < 4; i++) {
    MPI_Send((void*)&M[L*primeiro], L*C/4, MPI_INT, i, 0, MPI_COMM_WORLD);
    primeiro += C/4;
  }
  //printf("submeteu pedaco pra todos os slaves\n");
  
  //As primeiras C/4 colunas sao feitas pelo processo master
  ordena_colunas(M, L, C/4);
  calcula_mediana(M, vet, L, C/4);

  //Recebe os resultados dos outros processos e junta no vetor de medianas
  for(i=1; i<4; i++) {
    MPI_Recv((void*)&vet[i], 1, MPI_FLOAT, i, MPI_ANY_TAG, MPI_COMM_WORLD, NULL);
    //printf("recebeu mediana %.1lf\n", vet[i]);
  }
  //Escreve no arquivo de saida
    for(j=0; j<C; j++) 
      fprintf(arquivo_saida,"%.1f, ",vet[j]);
  fprintf(arquivo_saida, "\n");

  fclose(arquivo_entrada);
  fclose(arquivo_saida);

  free(vet);
  free(M);
  return 0;
    
}

void slave(int my_rank) {
  //printf("executando slave %d\n", my_rank);
  int source, linhas, colunas;
  MPI_Status status;
  int *rec_buff;
  float *medianas;
  int *matriz;
  
  rec_buff = (int*)malloc(sizeof(int)*2);
  MPI_Recv((void*)rec_buff, 2, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);//recebe numero de linhas e colunas
  //printf("slave %d recebeu %d linhas e %d colunas\n", my_rank, rec_buff[0], rec_buff[1]);

  source = status.MPI_SOURCE;
  linhas = rec_buff[0];
  colunas = rec_buff[1];
  free(rec_buff);

  //printf("worker %d recebeu %d linhas e %d colunas do source %d\n", my_rank, linhas, colunas, source);

  //printf("slave %d vai receber sua parte da matriz\n", my_rank);
  rec_buff = malloc(sizeof(int)*linhas*colunas);
  MPI_Recv((void*)rec_buff, linhas*colunas, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
  //printf("slave %d recebeu sua parte da matriz\n", my_rank);

  matriz = rec_buff;
  int i;


  ordena_colunas(rec_buff, linhas, colunas);
  medianas = (float*)malloc(sizeof(float)*colunas);
  calcula_mediana(rec_buff, medianas, linhas, colunas);
  
  /*for(i=0; i < colunas; i++) {
    printf("mediana %d do slave %d: %.1lf\n", i, my_rank, medianas[i]);
  }*/

  //printf("slave %d vai mandar de volta pro master\n", my_rank);
  MPI_Send((void*)medianas, colunas, MPI_FLOAT, source, 0, MPI_COMM_WORLD);
  //printf("slave %d terminou de executar\n", my_rank);

  free(medianas);
  free(rec_buff);
}