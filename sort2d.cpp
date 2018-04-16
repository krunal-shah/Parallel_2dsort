#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <bits/stdc++.h>
#include <mpi.h>
#include <algorithm>
#include <iterator>
#include <sys/stat.h>

using namespace std;

char* test_path;

int rank;
int size;
int num_cols;
int cols_per_proc;
int rows_per_proc;
int my_max_rows = 0;
int num_rows = 0;
int my_string_size = 0;
int ssize = 0;
int dsize = 0;
int rows;
int cols;
int base;
char **my_cols;
char *oned_cols;
char **my_rows;
char *oned_rows;
char *oned_rows_others;
char **others_rows;

size_t getFilesize(const char* filename) 
{
    struct stat st;
    if(stat(filename, &st) != 0) {
        return 0;
    }
    return st.st_size;   
}

int less_than_key(const void* lhs, const void* rhs) 
{ 
	float lhsf = *((float *)lhs);
	float rhsf = *((float *)rhs);
	int ret = (lhsf < rhsf)?-1:1;
	return ret;
}

void update_onedcols()
{
	int k = 0;
	for (int i = 0; i < rows; ++i)
	{
		for (int j = 0; j < cols_per_proc; ++j)
		{
			for (int l = 0; l < dsize; ++l)
			{
				oned_cols[k] = my_cols[j][i*dsize+l];
				k++;
			}
		}
	}
}

void update_myrows()
{
	for (int i = 0; i < rows_per_proc; ++i)
	{
		for (int j = 0; j < cols; ++j)
		{
			for (int l = 0; l < dsize; ++l)
			{
				my_rows[i][j*dsize + l] = oned_rows[((cols_per_proc*rows_per_proc)*(j/cols_per_proc) + (i)*(cols_per_proc) + j%cols_per_proc)*dsize + l];
			}
		}
	}
}


void update_othersrows()
{
	for (int i = 0; i < rows_per_proc; ++i)
	{
		for (int j = 0; j < cols; ++j)
		{
			for (int l = 0; l < dsize; ++l)
			{
				others_rows[i][j*dsize + l] = oned_rows_others[((cols_per_proc*rows_per_proc)*(j/cols_per_proc) + (i)*(cols_per_proc) + j%cols_per_proc)*dsize + l];
			}
		}
	}
}

void update_onedrows()
{
	int k = 0;
	for (int i = 0; i < cols; ++i)
	{
		for (int j = 0; j < rows_per_proc; ++j)
		{
			for (int l = 0; l < dsize; ++l)
			{
				oned_rows[k] = my_rows[j][i*dsize+l];
				k++;	
			}
		}
	}
}

void update_mycols()
{
	for (int i = 0; i < cols_per_proc; ++i)
	{
		for (int j = 0; j < rows; ++j)
		{
			for (int l = 0; l < dsize; ++l)
			{
				my_cols[i][j*dsize + l] = oned_cols[((cols_per_proc*rows_per_proc)*(j/rows_per_proc) + (i)*(rows_per_proc) + j%rows_per_proc)*dsize + l];
			}
		}
	}
}


int main(int argc, char* argv[])
{
	num_cols = atoi(argv[1]);
	
	MPI_Init(&argc, &argv);
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);

	cols_per_proc = (num_cols%size == 0)?(num_cols/size):((num_cols/size)+1);
	test_path = argv[2];
	
	// To calculate the maximum number of rows
	for (int i = rank*cols_per_proc + 1; i <= (rank+1)*cols_per_proc ; ++i)
	{
		// Compute the file path
		char file_name[20];
		sprintf(file_name, "%d", i);
 		char *file_path = (char*)malloc(strlen(test_path) + 20);
		strcpy(file_path, test_path);
		strcat(file_path, file_name);
		
		// Open file
		FILE *input_file;
		input_file = fopen(file_path,"rb");

		if(input_file == NULL)
		{
			// printf("Cannot open file %s\n", file_path);
		}
		else
		{
			if(fread(&my_string_size, sizeof(int), 1, input_file) != 1)
			{
				//printf("%s is empty\n", file_path);
			}
				
			// cout << getFilesize(file_path) << endl;
			int tr = getFilesize(file_path) - sizeof(int);

			int ter = (tr)/(long long int)(sizeof(float) + my_string_size);
			if(ter > my_max_rows)
				my_max_rows = ter;

			fclose(input_file);
		}
	}


	MPI_Allreduce(&my_max_rows,&num_rows,1,MPI_INT,MPI_MAX,MPI_COMM_WORLD);
	MPI_Allreduce(&my_string_size,&ssize,1,MPI_INT,MPI_MAX,MPI_COMM_WORLD);
	
	dsize = sizeof(float) + ssize + 1;
	rows_per_proc = (num_rows%size == 0)?(num_rows/size):((num_rows/size)+1);
	rows = rows_per_proc*size;
	cols = cols_per_proc*size;
	base = rank*cols_per_proc + 1;
	my_cols = (char **)malloc(sizeof(char *)*cols_per_proc);
	for (int i = 0; i < cols_per_proc; ++i)
	{
		my_cols[i] = (char *)malloc(sizeof(char)*rows*dsize);
	}
	
	for (int i = rank*cols_per_proc + 1; i <= (rank+1)*cols_per_proc ; ++i)
	{
		char file_name[20];
		sprintf(file_name, "%d", i);
		char *file_path = (char*)malloc(strlen(test_path) + 20);
		strcpy(file_path, test_path);
		strcat(file_path, file_name);
		
		FILE *input_file;
		input_file = fopen(file_path,"rb");

		if(input_file == NULL)
		{	
			// printf("Cannot open file %s\n", file_path);
			for (int j = 0; j < rows; ++j)
			{
				float* fptr;
				fptr = (float *)(&my_cols[i-base][dsize*j]);
				*fptr = INFINITY;
				my_cols[i-base][dsize*j + sizeof(float)] = '\0';
			}
		}
		else
		{
			int n;
			if(fread(&n, sizeof(int), 1, input_file) != 1)
			{
				// printf("%s is empty\n", file_path);
			}
			
			float c;
			char ch[n];
			int count = 0;
			while(fread(&c, sizeof(float), 1, input_file) == 1)
			{
				float* fptr;
				fptr = (float *)(&my_cols[i-base][count*dsize]);
				*fptr = c;

				if(fread(ch, n*sizeof(char), 1, input_file) == 1)
				{
					int j = 0;
					for (; j < n; ++j)
					{
						my_cols[i-base][count*dsize + sizeof(float) + j] = ch[j];
					}
					my_cols[i-base][count*dsize + sizeof(float) + n] = '\0';
					count++;
				}
			}
			while(count < rows)
			{
				float* fptr;
				fptr = (float *)(&my_cols[i-base][count*dsize]);
				*fptr = INFINITY;
				my_cols[i-base][dsize*count + sizeof(float)] = '\0';

				count++;	
			}
			fclose(input_file);
		}
	}

	oned_cols = (char *)malloc(sizeof(char)*cols_per_proc*rows*dsize);
	oned_rows = (char *)malloc(sizeof(char)*cols_per_proc*rows*dsize);
	my_rows = (char **)malloc(sizeof(char *)*rows_per_proc);
	for (int i = 0; i < rows_per_proc; ++i)
	{
		my_rows[i] = (char *)malloc(sizeof(char)*cols*dsize);
	}
	int col_check = 1;
	int row_check = 1;
	int iteration = 0;

	while(1)
	{
		update_onedcols();		

		MPI_Alltoall(oned_cols, cols_per_proc*rows_per_proc*dsize, MPI_CHAR, 
					oned_rows, cols_per_proc*rows_per_proc*dsize, MPI_CHAR, MPI_COMM_WORLD);

		update_myrows();
		
		int temp_row_check = 0;
		row_check = 1;
		for (int i = 0; i < rows_per_proc; ++i)
		{
			for (int j = 0; j < cols - 1; ++j)
			{
				float x = *((float *)&my_rows[i][j*dsize]);
				float y = *((float *)&my_rows[i][(j+1)*dsize]);
				if(x > y)
				{
					temp_row_check = 1;
				}
			}
		}
		
		MPI_Allreduce(&temp_row_check,&row_check,1,MPI_INT,MPI_MAX,MPI_COMM_WORLD);
		
		if(row_check == 0 && iteration > 0)
		{
			break;
		}

		for (int i = 0; i < rows_per_proc; ++i)
		{
			qsort(my_rows[i], cols, dsize, less_than_key);
		}

		update_onedrows();

		MPI_Alltoall(oned_rows, cols_per_proc*rows_per_proc*dsize, MPI_CHAR, 
					oned_cols, cols_per_proc*rows_per_proc*dsize, MPI_CHAR, MPI_COMM_WORLD);
		

		update_mycols();

		int temp_col_check = 0;
		col_check = 1;
		for (int i = 0; i < cols_per_proc; ++i)
		{
			for (int j = 0; j < rows - 1; ++j)
			{
				float x = *((float *)&my_cols[i][j*dsize]);
				float y = *((float *)&my_cols[i][(j+1)*dsize]);
				if(x > y)
				{
					temp_col_check = 1;
				}
			}
		}

		MPI_Allreduce(&temp_col_check,&col_check,1,MPI_INT,MPI_MAX,MPI_COMM_WORLD);
		
		if(col_check == 0)
		{
			break;
		}
		
		for (int i = 0; i < cols_per_proc; ++i)
		{
			qsort(my_cols[i], rows, dsize, less_than_key);
		}

		iteration++;
	}


	// File writing
	char file_name[20];
	int i = 0;
	sprintf(file_name, "%d", i);
	char *file_path = (char*)malloc(strlen(test_path) + 20);
	strcpy(file_path, test_path);
	strcat(file_path, file_name);
	FILE *output_file;
	output_file = fopen(file_path,"wb");
		

	for (int l = 0; l < size; ++l)
	{
		MPI_Barrier(MPI_COMM_WORLD);
		if(rank == l)
		{
			FILE *output_file;
			output_file = fopen(file_path,"a+b");
			for (int i = 0; i < rows_per_proc; ++i)
			{
				for (int j = 0; j < cols; ++j)
				{
					if((*(float *)(&my_rows[i][j*dsize])) == INFINITY)
					{
						continue;
					}
					fwrite(((float *)(&my_rows[i][j*dsize])), sizeof(float), 1, output_file);
					// printf("%f ", (*(float *)(&my_rows[i][j*dsize])));
					
					int m = 0;
					while(my_rows[i][j*dsize + sizeof(float) + m] != '\0')
					{
						fwrite(&my_rows[i][j*dsize + sizeof(float) + m], sizeof(char), 1, output_file);
						// printf("%d = %c, ", m, my_rows[i][j*dsize+ sizeof(float) + m]);
						
						m++;
					}
					// printf("\n");
				}
			}	
			fclose(output_file);
		}
	}

	// Alternate way to write file
	// if(rank != 0)
	// {
	// 	MPI_Send(oned_rows, cols_per_proc*rows*dsize, MPI_CHAR, 0, rank, MPI_COMM_WORLD);
	// }
	
	// if(rank == 0)
	// {
	// 	char file_name[20];
	// 	int i = 0;
	// 	sprintf(file_name, "%d", i);
		
	// 	char *file_path = (char*)malloc(strlen(test_path) + 20);
	// 	strcpy(file_path, test_path);
	// 	strcat(file_path, file_name);

	// 	FILE *output_file;
	// 	output_file = fopen(file_path,"wb");

	// 	oned_rows_others = (char *)malloc(sizeof(char)*cols_per_proc*rows*dsize);
	// 	others_rows = (char **)malloc(sizeof(char *)*rows_per_proc);
	// 	for (int i = 0; i < rows_per_proc; ++i)
	// 	{
	// 		others_rows[i] = (char *)malloc(sizeof(char)*cols*dsize);
	// 	}

	// 	for (int i = 0; i < rows_per_proc; ++i)
	// 	{
	// 		for (int j = 0; j < cols; ++j)
	// 		{
	// 			if((*(float *)(&my_rows[i][j*dsize])) == INFINITY)
	// 			{
	// 				continue;
	// 			}
	// 			fwrite(((float *)(&my_rows[i][j*dsize])), sizeof(float), 1, output_file);
	// 			// printf("%f ", (*(float *)(&my_rows[i][j*dsize])));
				
	// 			int m = 0;
	// 			while(my_rows[i][j*dsize + sizeof(float) + m] !='\0')
	// 			{
	// 				fwrite(&my_rows[i][j*dsize + sizeof(float) + m], sizeof(char), 1, output_file);
	// 				// printf("%d = %c, ", m, my_rows[i][j*dsize+ sizeof(float) + m]);
	// 				m++;
	// 			}
	// 			// printf("\n");
	// 		}
	// 	}

	// 	for (int l = 1; l < size; ++l)
	// 	{
	// 		MPI_Recv(oned_rows_others, cols_per_proc*rows*dsize, MPI_CHAR, l, l, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	// 		update_othersrows();
			
	// 		for (int i = 0; i < rows_per_proc; ++i)
	// 		{
	// 			for (int j = 0; j < cols; ++j)
	// 			{
	// 				if((*(float *)(&others_rows[i][j*dsize])) == INFINITY)
	// 				{
	// 					continue;
	// 				}
	// 				fwrite(((float *)(&others_rows[i][j*dsize])), sizeof(float), 1, output_file);
	// 				// printf("%f ", (*(float *)(&others_rows[i][j*dsize])));
					
	// 				int m = 0;
	// 				while(others_rows[i][j*dsize + sizeof(float) + m] !='\0')
	// 				{
	// 					fwrite(&others_rows[i][j*dsize + sizeof(float) + m], sizeof(char), 1, output_file);
	// 					// printf("%d = %c, ", m, others_rows[i][j*dsize+ sizeof(float) + m]);
	// 					m++;
	// 				}
	// 			}
	// 		}
	// 	}
	// 	fclose(output_file);
	// }	


	
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
}
