// sortbigfile1.cpp: определяет точку входа для консольного приложения.
//

// for win32
#ifndef _WIN32_WINNT            
#define _WIN32_WINNT 0x0600     
#endif

#include <iostream>
#include <string>
#include <vector>

#define  BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
namespace fs = boost::filesystem;
//#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

#include "splitfile.h"
#include "mergefiles.h"

// сортировка больших файлов
// 1
// в одном потоке последовательно читаем входной файл в буфер.
// когда буфер наполнился сортируем его (многопоточно) и записываем на диск во временный файл.
// повторяем до достижения конца файла.
// 2
// последовательно читаем два временных (отсортированых) файла, производим их слияние  (многопоточно)
// записываем на диск суммарный файл. удаляем два исходных. повторяем пока не останется один файл.
// 

int main(int argc, char * argv[])
{	
	if(argc!=3)
	{
		std::cout<<"usage: sortbigfile.exe infile outfile\n";
		return 0;
	}
	std::string in_file(argv[1]);
	std::string out_file(argv[2]);

	if(!fs::exists(in_file))
	{
		std::cout<<"error:in file: "<<in_file<<" not exist \n";
		return 0; 
	}
	
	// размер файла должен быть кратен 4 байтам
	if(fs::file_size(in_file)%4 != 0)
	{
		std::cout<<"error:in file size must div by 4\n";
		return 0;
	}

	long max_memory_size = 256*1024*1024; // 256 MB	
    //hardware_concurrency() - The number of hardware threads available on the current system 
	//(e.g. number of CPUs or cores or hyperthreading units), or 0 if this information is not available.
	int  thread_pool_size = 2*boost::thread::hardware_concurrency(); 	
	if (thread_pool_size ==0) thread_pool_size=2;

	boost::asio::io_service io_service;
	std::vector<std::string> temp_files_vec;

	// разбить файл на сортированые временные файлы
	SplitFile split(max_memory_size, io_service);

	if (split.SplitBigFiletoSortedFiles<unsigned int>(in_file, temp_files_vec, thread_pool_size) != 1)
	{
		std::cout<<"error in SplitBigFiletoSortedFiles - exit\n";
		return 0;
	}

	// TEST	
	/*
	for(int i=0;i<16;i++)
	{
			std::string tmp_file_name = "temp"+boost::lexical_cast<std::string>(temp_files_vec.size())+".dat";
			temp_files_vec.push_back(tmp_file_name);
	}	
	// TEST

	for(int i=0;i<temp_files_vec.size();i++)
	{
		std::cout<<temp_files_vec[i]<<'\n';
	}
	*/

	// слияние временных файлов в один
	MergeFiles merge(max_memory_size,io_service);
	
	if (merge.MergeFilestoOne<unsigned int>(temp_files_vec, out_file, thread_pool_size) != 1)
	{
		std::cout<<"error in MergeFilestoOne - exit\n";
		return 0;
	}

	return 0;
}

