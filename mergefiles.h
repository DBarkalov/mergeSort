#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#define  BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
namespace fs = boost::filesystem;
#include "boost/lexical_cast.hpp"
#include <boost/scoped_array.hpp>

#include "merge.h"


/// Слияние предварительно отсортированых файлов в один
class MergeFiles
{

public:
	MergeFiles(long max_mem_size, boost::asio::io_service& ios):memory_buffer_size(max_mem_size),io_service(ios)
	{
	}

	// слияние всех файлов в один выходной файл
	template< class _Type>
	bool MergeFilestoOne(std::vector<std::string> &in_files_vec, std::string output_file_name, int thread_pool_size)
	{	
		int number_of_in_files = in_files_vec.size(); // сохранено для формирования имен вр файлов

		while(in_files_vec.size()>=2)
		{
			// файлы для слияния
			std::string tmp_in_file1 = *in_files_vec.begin();    
			std::string tmp_in_file2 = *(in_files_vec.begin()+1);

			// имя выходного файла
			std::string tmp_out_file;
			if((in_files_vec.size()) == 2)
			{
				tmp_out_file = output_file_name;
			} else 
			{
				tmp_out_file = "temp"+boost::lexical_cast<std::string>(number_of_in_files++)+".dat";
			}

			// слияние
			if (MergeTwoFiles<_Type>(tmp_in_file1, tmp_in_file2, tmp_out_file, thread_pool_size) != 1 )
			{ 
				std::cout<<"error: in MergeTwoFiles\n";
				return 0;
			}

			// удалить два файла с диска
			boost::filesystem::remove(*in_files_vec.begin());
			boost::filesystem::remove(*(in_files_vec.begin()+1));

			// удалить два файла из списка
			in_files_vec.erase(in_files_vec.begin(),in_files_vec.begin()+2);

			in_files_vec.push_back(tmp_out_file);			
			
		}		
		
		return 1;
	}

private:
	/// слияние двух файлов в один
	template< class _Type>
	bool MergeTwoFiles(std::string in_file_name1, std::string in_file_name2, std::string out_file_name, int thread_pool_size)
	{		 
		
		// размеры файлов
		__int64 infile_size1 = 0;
		__int64 infile_size2 = 0;

		try 
		{
			infile_size1 = fs::file_size(in_file_name1);
			infile_size2 = fs::file_size(in_file_name2);
		}
		catch(...)
		{
			std::cout<<"error:MergeTwoFiles define file size\n";
			return 0;
		}

		
		// буферы
		long  InBufSize  = memory_buffer_size/2;
		long  OutBufSize = memory_buffer_size/2;

		// выделить память
		_Type *pInBuf = 0;
		_Type *pOutBuf = 0;
		try
		{
			pInBuf = new _Type[InBufSize/sizeof(_Type)];
			pOutBuf = new _Type[OutBufSize/sizeof(_Type)];
		} catch(char * str)
		{
			std::cout << "MergeTwoFiles memory error: " << str << '\n';
			return 0;
		}

		boost::scoped_array<_Type> saInBuf (pInBuf);
		boost::scoped_array<_Type> saOutBuf (pOutBuf);


		Merge merge(io_service, thread_pool_size);

		std::ifstream infile1 (in_file_name1.c_str(),std::ofstream::binary);
		std::ifstream infile2 (in_file_name2.c_str(),std::ofstream::binary);		
		std::ofstream outfile (out_file_name.c_str(),std::ofstream::binary);

		// из каждого файла читаем максимум по memory_buffer_size/4
		// сливаем из двух файлов
		while(infile_size1>0 && infile_size2>0)
		{			
			long read_buffer_size1 = (infile_size1>=InBufSize/2)?InBufSize/2:infile_size1; 
			long read_buffer_size2 = (infile_size2>=InBufSize/2)?InBufSize/2:infile_size2;

			// читать часть файла 1 в буфер
			ReadFiletoBuffer(infile1,saInBuf.get(),read_buffer_size1);

			// читать часть файла 2 в буфер
			ReadFiletoBuffer(infile2, saInBuf.get()+read_buffer_size1/4, read_buffer_size2);  

			long p1 = 0;
			long r1 = read_buffer_size1/4-1;
			long p2 = read_buffer_size1/4;
			long r2 = p2+read_buffer_size2/4-1;

			// merge
			merge.StartParallelMerge(saInBuf.get(), p1, r1, p2, r2,saOutBuf.get(),0);

			// дописать pOutBuf в выходной файл
			outfile.write((char*)saOutBuf.get(),(read_buffer_size1+read_buffer_size2));

			infile_size1-=read_buffer_size1;
			infile_size2-=read_buffer_size2;
		}


		while(infile_size1>0)
		{
			long read_buffer_size1 = (infile_size1>=InBufSize)?InBufSize:infile_size1; // 67108864
			ReadFiletoBuffer(infile1,saInBuf.get(),read_buffer_size1);
			outfile.write((char*)saInBuf.get(),read_buffer_size1);		
			infile_size1-=read_buffer_size1;
		}

		while(infile_size2>0)
		{
			long read_buffer_size2 = (infile_size2>=InBufSize)?InBufSize:infile_size2; // 67108864
			ReadFiletoBuffer(infile2,saInBuf.get(),read_buffer_size2);
			outfile.write((char*)saInBuf.get(),read_buffer_size2);	
			infile_size2-=read_buffer_size2;		
		}

		infile1.close();
		infile2.close();
		outfile.close();	

		return 1;
	}

	template< class _Type>
	void ReadFiletoBuffer(std::ifstream &infile, _Type *pInts, long buf_size)
	{
		// здесь можно читать блоками по 64К может быть быстрее
		infile.read((char*)pInts, buf_size);	
	}


private:	
	long memory_buffer_size;
	boost::asio::io_service& io_service;
};
