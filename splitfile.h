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
// qsort
#include "qsort.h"


/// разбить большой файл на отсортированные файлы
class SplitFile
{	

public:
	SplitFile(long max_mem_size, boost::asio::io_service& ios):memory_buffer_size(max_mem_size),io_service(ios)
	{
	}	


	// разбить большой файл на сортированые куски
	template< class _Type>
	bool SplitBigFiletoSortedFiles(std::string in_filename, std::vector<std::string> &out_files_vec, int thread_pool_size)
	{	
		 out_files_vec.clear();

		 __int64 filesize = fs::file_size(in_filename);

		 
		 // выделить память
		 _Type *p_int = 0;
		 try
		 {
			p_int = new _Type[memory_buffer_size/sizeof(_Type)];
		 } catch(char * str)
		 {
			std::cout << "SplitBigFiletoSortedFiles memore error: " << str << '\n';
			return false;
		 }

   		 // буфер
		 boost::scoped_array<_Type> saBuf (p_int);
		 
		 std::ifstream infile (in_filename.c_str(),std::ofstream::binary);
		 
		 QSort qs(io_service,thread_pool_size);

		 while(filesize>0)
		 {
			 long read_buffer_size = (filesize>=memory_buffer_size)?memory_buffer_size:filesize;
			 			 
			 // читать часть файла в буфер
			 ReadFiletoBuffer(infile,saBuf.get(),read_buffer_size);
			
			 // сортировка
			 qs.StartParallelQSort<_Type>(saBuf.get(),0,(read_buffer_size/sizeof(_Type))-1);

			 // записать во временный файл
			 std::string tmp_file_name = "temp"+boost::lexical_cast<std::string>(out_files_vec.size())+".dat";
			 WriteTempFile(saBuf.get(),read_buffer_size,tmp_file_name);
			 out_files_vec.push_back(tmp_file_name);
		
			 filesize-=read_buffer_size;
		 }		
		
	     infile.close();

		 return true;
	}

private:
	template< class _Type>
	void WriteTempFile(_Type *pInts, long buf_size, std::string filename)
	{		
		std::ofstream file (filename.c_str(),std::ofstream::binary);
		file.write((char*)pInts,buf_size);
		file.close();
	}

	template< class _Type>
	void ReadFiletoBuffer(std::ifstream &infile, _Type *pInts, long buf_size)
	{
		// здесь можно читать блоками по 64К может быть быстрее
		infile.read((char*)pInts, buf_size);		
		//std::cout<<"ReadFiletoBuffer  ok\n";
	}
	
private:	
	long memory_buffer_size;
	boost::asio::io_service& io_service;
};

