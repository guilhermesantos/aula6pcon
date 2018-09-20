#include <mpi.h>
#include <stdlib.h>

void quicksort(int *, int, int, int);
int partition (int *, int, int, int);
void ordena_colunas(int *M, int L, int C);
void calcula_mediana(int *M, float *vet, int L, int C);

int main(int argc, char **argv) {
	int npes; 
	int my_rank;
	int *rec_buff = (int*)malloc(sizeof(int)*2);
	int *send_buff;
	int num_linhas, num_colunas;
	int count;
	MPI_Status status;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &npes);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);



	MPI_Recv((void*)rec_buff, 2, MPI_INT, MPI_ANY_SOURCE, 
		MPI_ANY_TAG, MPI_COMM_WORLD, &status);//recebe numero de linhas e colunas

	int source = status.MPI_SOURCE;
	int linhas = rec_buff[0];
	int colunas = rec_buff[1];

	free(rec_buff);
	rec_buff = malloc(sizeof(int)*linhas*colunas);

	MPI_Recv((void*)rec_buff, linhas*colunas, MPI_INT, MPI_ANY_SOURCE, 
		MPI_ANY_TAG, MPI_COMM_WORLD, &status);

	ordena_colunas(rec_buff, linhas, colunas);

	float *medianas = (float*)malloc(sizeof(float)*colunas);
	calcula_mediana(rec_buff, medianas, linhas, colunas);

	MPI_Send((void*)medianas, colunas, MPI_FLOAT, source, 0, MPI_COMM_WORLD);
	
	free(medianas);
	free(rec_buff);

	MPI_Finalize();
}

void ordena_colunas(int *M, int L, int C) 
{
  int j;
  
  for (j = 0; j < C; j++) {
      //manda o endereco do primeiro elemento da coluna, limites inf e sup e a largura da matriz
      quicksort(&M[j], 0, L-1, C);
  }
} // fim ordena_colunas

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