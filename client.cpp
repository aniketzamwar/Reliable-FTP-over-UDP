/*
 * Client.cpp
 * File Sender
 *
 * Code written by Aniket Zamwar
 * zamwar@usc.edu
 * designed by Aniket and Abhishek
 */

#include "client.h"

extern FILE *log_fp;
extern pthread_mutex_t logger_file_lock;
uint64_t total_bytes_sent = 0;
uint64_t total_bytes_received = 0;
int timer_resender_exit_status = 0;
int received_state=0;
struct hostent *server;
time_t start_time,end_time;

/*
 * Functions Declaration
 */
void init();
void timer(void *);
void receiver(void *);
void sender(void *);
void resender(void *);
extern struct message* stream_to_message(char *,int);
extern char* message_to_stream(struct message);
extern void error(const char *);
extern void logger_system(struct message *, int);

struct sockaddr_in serv_addr;
struct stat fileData;


/*
 * Main of Client Starts here
 */
int main(int argc, char *argv[])
{
	log_fp = fopen("time_elapsed_file_sender.log","w+");
	if(log_fp == NULL)
		printf("Could not create time elapsed file\n");

	int portno, n;
	//uint64_t send_soc_buffer_size = 64000;
	uint64_t recv_sock_buffer_size = 1000000000;	//1024x1024x1024

	socklen_t fromLen;
	char buffer[256];
	if (argc < 3) {
		fprintf(stderr,"usage %s hostname udp_port filename\n", argv[0]);
		exit(0);
	}
	portno = atoi(argv[2]);
	client_udp_socket = socket(AF_INET, SOCK_DGRAM, 0);
	if (client_udp_socket < 0)
		error("ERROR opening socket");

	//setsockopt(client_udp_socket,SOL_SOCKET,SO_SNDBUF,&send_soc_buffer_size, sizeof(send_soc_buffer_size));

	server = gethostbyname(argv[1]);
	if (server == NULL) {
		fprintf(stderr,"ERROR, no such host\n");
		exit(0);
	}
	bzero((char *) &serv_addr, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr,
			(char *)&serv_addr.sin_addr.s_addr,
			server->h_length);
	serv_addr.sin_port = htons(portno);
	server_udp_sock_len = sizeof(serv_addr);

	// open file to be read and send
	strcpy(file_path,argv[3]);
	fp = fopen(file_path, "r");
	if(fp == NULL){
		error("\nError File not found");
		exit(0);
	}
	// get file size and stats
	if(stat(file_path,&fileData) < 0){
		printf("\nThe specified ini file %s does not exist\n",file_path);
		exit(0);
	}

	init();

	//create threads
	pthread_create(&sender_thread,NULL,(void* (*)(void*))sender,NULL);
	pthread_create(&timer_thread,NULL,(void* (*)(void*))timer,NULL);
	pthread_create(&receiver_thread,NULL,(void* (*)(void*))receiver,NULL);
	pthread_create(&resender_thread,NULL,(void* (*)(void*))resender,NULL);

	//wait for threads to exit
	pthread_join(sender_thread,NULL);
	pthread_join(timer_thread,NULL);
	pthread_join(receiver_thread,NULL);
	pthread_join(resender_thread,NULL);
	return 1;
}

void init()
{
	// initial parameters to be set.
	cwnd_size = 20;
	cwnd_current_count = 0;
	curr_seq_num = 0;
	while(!queue_of_sent_seq.empty()){
		queue_of_sent_seq.pop_back();
	}
	while(!udp_control_queue.empty()){
		udp_control_queue.pop_back();
	}
	for(MsgMapIter = MsgMap.begin() ; MsgMapIter != MsgMap.end() ; MsgMapIter++){
		MsgMap.erase(MsgMapIter->first);
	}
	queue_of_sent_seq.clear();
	MsgMap.clear();

}


/*
 * Resends the packets that have expired and not received ACK from receiver.
 * This thread is signalled by timer everytime, timer comes across expired unacknowledged packets.
 */
void resender(void *param){

	uint64_t current_seq_no_to_be_sent = 0;
	char *message_stream;

	while(1){

		pthread_mutex_lock(&udp_control_queue_Lock);

		//if all acks received, exit.
		if(timer_resender_exit_status == 1){
			pthread_mutex_unlock(&udp_control_queue_Lock);
			break;
		}
		if(udp_control_queue.empty()){
			// if nothing in queue wait
			pthread_cond_wait(&udp_control_queue_CV,&udp_control_queue_Lock);
		}
		if(timer_resender_exit_status == 1){
			pthread_mutex_unlock(&udp_control_queue_Lock);
			break;
		}
		pthread_mutex_lock(&MsgMap_lock);

		udp_control_queue_itr = udp_control_queue.begin();
		while(udp_control_queue_itr < udp_control_queue.end()){
			current_seq_no_to_be_sent = *udp_control_queue_itr;
			udp_control_queue.erase(udp_control_queue.begin());
			udp_control_queue_itr = udp_control_queue.begin();
			if((MsgMapIter = MsgMap.find(current_seq_no_to_be_sent)) != MsgMap.end()){

				//Need to resend
				struct message_info *message_info_ptr = MsgMapIter->second;

				struct message message_struct;
				message_struct.type = DATA;
				message_struct.seq_num = current_seq_no_to_be_sent;
				message_struct.data = (char *) malloc(PAYLOAD_SIZE + 1);
				memset(message_struct.data,'\0',PAYLOAD_SIZE + 1);

				fseek(fp,message_info_ptr->file_offset,SEEK_SET);
				int bytes_read = fread(message_struct.data,1,PAYLOAD_SIZE,fp);
				message_struct.length = bytes_read + 5;
				message_stream = message_to_stream(message_struct);

				int n = sendto(client_udp_socket,message_stream,message_struct.length,0,(struct sockaddr *) &serv_addr,sizeof(serv_addr));
				if(message_struct.data != NULL) free(message_struct.data);
				if(message_stream != NULL) free(message_stream);
				if(n < 0) error("\nError Sending over send to\n");

				gettimeofday(&message_info_ptr->timestamp,NULL);
				// As recently sent, push to queue
				queue_of_sent_seq.push_back(current_seq_no_to_be_sent);

			} // end if
		} // end queue while loop
		pthread_mutex_unlock(&MsgMap_lock);
		pthread_mutex_unlock(&udp_control_queue_Lock);
	}// end while loop
	pthread_exit(NULL);
}


/*
 * Timer Function
 * According to time span, checks all the seq no in the window.
 * If any message have expired, puts the sequence no. to the queue
 * and signals sender.
 */
void timer(void *param)
{
	uint32_t expire_time = 300000;
	time_t current_system_time;
	uint64_t resend_seq[WINDOW_SIZE];
	uint64_t no_of_resend_values = 0;

	struct timeval currtime;
	struct timeval tvtime;


	while(1){

		no_of_resend_values = 0;
		tvtime.tv_sec = 0;
		tvtime.tv_usec = 50000;

		//Sleep for some time
		select(0,NULL,NULL,NULL,&tvtime);

		pthread_mutex_lock(&udp_control_queue_Lock);
		pthread_mutex_lock(&MsgMap_lock);

		// wait if window is empty
		if(notify_status == 1){
			// Done with Transfer
			timer_resender_exit_status = 1;
			pthread_mutex_unlock(&MsgMap_lock);
			pthread_cond_signal(&udp_control_queue_CV);
			pthread_mutex_unlock(&udp_control_queue_Lock);
			break;
		}

		queue_of_sent_seq_itr = queue_of_sent_seq.begin();
		// Remove sequence number from, time stamp ordered sequence number
		while(queue_of_sent_seq_itr < queue_of_sent_seq.end()){

			if((MsgMapIter = MsgMap.find((*queue_of_sent_seq_itr))) == MsgMap.end()){
				// Not Found in Map, means Receiver got the ACK and it was removed from the Map
				// so discard this sequence number and erase from queue
				queue_of_sent_seq.erase(queue_of_sent_seq.begin());
				queue_of_sent_seq_itr = queue_of_sent_seq.begin();
			}
			else{
				// Found in Map, means Receiver has not yet received ACK and so need to check if the packet needs to be resent.
				// Check Timestamp
				gettimeofday(&currtime,NULL);
				uint32_t diff_time = currtime.tv_usec - MsgMapIter->second->timestamp.tv_usec;
				if( diff_time > expire_time){
					// packet has expired and so need to put to resend queue
					udp_control_queue.push_back(*queue_of_sent_seq_itr);
					//erase from here, as sender will put it again in this queue, as this queue is sorted with timestamp
					queue_of_sent_seq.erase(queue_of_sent_seq.begin());
					queue_of_sent_seq_itr = queue_of_sent_seq.begin();
				}
				else{
					// as the packet is not expired and as the queue is sorted with timestamp,
					// no further packet can be expired, therefore we break from loop of queue
					//printf("\nNot Expired, so break %d\n",no_of_resend_values);
					break;
				}
			}//end else of found in map
		}//end while for queue
		pthread_mutex_unlock(&MsgMap_lock);

		if(!udp_control_queue.empty()){
			// if not empty, signal and inform resender
			pthread_cond_signal(&udp_control_queue_CV);
		}
		pthread_mutex_unlock(&udp_control_queue_Lock);

	} // end while loop
	pthread_exit(NULL);
}

void send_notify_message(){

	struct message_info *message_info_ptr = (struct message_info *) malloc(sizeof(struct message_info));
	message_info_ptr->seq_num = curr_seq_num;

	struct message message_struct;
	message_struct.type = NOTIFY;
	message_struct.seq_num = curr_seq_num;
	message_struct.data = (char*)malloc(3 + 1);

	memset(message_struct.data,'\0',3 + 1);
	strcpy(message_struct.data, "END");

	message_struct.length = 8;
	char *message_stream = message_to_stream(message_struct);
	int i =0,n;
	while(i++ < 10)
		n = sendto(client_udp_socket,message_stream,message_struct.length,0,(struct sockaddr *) &serv_addr,sizeof(serv_addr));

	if(message_struct.data != NULL) free(message_struct.data);
	if(message_stream != NULL) free(message_stream);
	if(n < 0){
		error("\nError Sending over send to: ");
	}

}

/*
 * Receiver Function
 * According to received Ack, removes the sequence no. from window and map.
 * If window counter is equal to window size, decrement window counter and signal
 * sender.
 */
void receiver(void *param)
{
	int n;
	struct sockaddr_in tcp_server_addr;
	char buffer[500000];
	struct message *ptr = NULL;

	while(1){

		memset(buffer,'\0',500000);

		if(total_bytes_received >= fileData.st_size){
			notify_status = 1;
			break;
		}

		n = recvfrom(client_udp_socket,buffer,1500,0,(struct sockaddr *) &server_addr,&server_udp_sock_len);
		received_state = 1;

		if (n < 0)
			error("ERROR reading from socket");
		ptr = stream_to_message(buffer,n);

		// process char stream to structure
		if(ptr->type == ACK){

			uint64_t ack_seq_num;
			int ack_seq_count = 0;
			char *pch,*str;
			str = ptr->data;

			pthread_mutex_lock(&udp_control_queue_Lock);
			pthread_mutex_lock(&MsgMap_lock);
			received_state = 0;
			pch = strtok (str,",");
			while(pch != NULL){

				ack_seq_num = atoi(pch);
				pch = strtok(NULL,",");

				udp_control_queue_itr = udp_control_queue.begin();
				while(udp_control_queue_itr < udp_control_queue.end())
				{
					if(*udp_control_queue_itr == ack_seq_num){
						udp_control_queue.erase(udp_control_queue_itr);
						break;
					}
					else{
						udp_control_queue_itr++;
					}
				}

				if((MsgMapIter = MsgMap.find(ack_seq_num)) != MsgMap.end()){

					total_bytes_received = total_bytes_received + PAYLOAD_SIZE;
					cwnd_current_count--;
					struct message_info *temp_ptr;
					temp_ptr = MsgMapIter->second;
					MsgMap.erase(MsgMapIter);
					if(temp_ptr!=NULL) free(temp_ptr);
				}
			}
			if(total_bytes_received >= fileData.st_size){

				send_notify_message();
				end_time = time(NULL);
				uint32_t time_diff = end_time - start_time;
				fprintf(log_fp,"%d\n",time_diff);
				fclose(log_fp);
				notify_status = 1;
				pthread_cond_signal(&MsgMap_CV);
				pthread_mutex_unlock(&MsgMap_lock);
				pthread_mutex_unlock(&udp_control_queue_Lock);
				break;
			}
			else{
				pthread_cond_signal(&MsgMap_CV);
				pthread_mutex_unlock(&MsgMap_lock);
				pthread_mutex_unlock(&udp_control_queue_Lock);
			}
		}
		if(ptr->data != NULL) free(ptr->data);
		if(ptr != NULL) free(ptr);
	}
	pthread_exit(NULL);
}


/*
 * Sender Function
 * According to the window, sends packet with sequence number.
 * Also, updates the window & map with new sequence number and updated time of packet sent
 * if current window counter is equal to window size, wait for signal from receiver
 */
void sender(void *param)
{
	//printf("\nSender Thread Started\n");
	char *message_stream;
	struct timeval tvtime;
	tvtime.tv_sec = 0;
	tvtime.tv_usec = 50000;

	start_time = time(NULL);
	while(1)
	{
		int  no_of_elements_sent = 0;
		uint64_t current_seq_no_to_be_sent = 0;

		int to_be_sent_queue[WINDOW_SIZE];
		int to_be_sent_count = 0;

		//acquire lock
		pthread_mutex_lock(&MsgMap_lock);
		if(notify_status == 1 || total_bytes_sent >= fileData.st_size){
			pthread_mutex_unlock(&MsgMap_lock);
			break;
		}

		if(cwnd_current_count == WINDOW_SIZE){
			pthread_cond_wait(&MsgMap_CV,&MsgMap_lock);
		}

		while(cwnd_current_count < WINDOW_SIZE && total_bytes_sent < fileData.st_size && received_state != 1){
			curr_seq_num++;

			cwnd_current_count++;

			struct message_info *message_info_ptr = (struct message_info *) malloc(sizeof(struct message_info));
			message_info_ptr->seq_num = curr_seq_num;
			message_info_ptr->file_offset = (message_info_ptr->seq_num - 1) * PAYLOAD_SIZE;

			struct message message_struct;
			message_struct.type = DATA;
			message_struct.seq_num = curr_seq_num;
			message_struct.data = (char*)malloc(PAYLOAD_SIZE + 1);
			memset(message_struct.data,'\0',PAYLOAD_SIZE + 1);

			fseek(fp,message_info_ptr->file_offset,SEEK_SET);
			//printf("\n[Sender] SET File pointer at offset %d",message_info_ptr->file_offset);

			int bytes_read = fread (message_struct.data,1,PAYLOAD_SIZE,fp);
			total_bytes_sent = total_bytes_sent + bytes_read;

			message_struct.length = bytes_read + 5;
			message_stream = message_to_stream(message_struct);

			int n = sendto(client_udp_socket,message_stream,message_struct.length,0,(struct sockaddr *) &serv_addr,sizeof(serv_addr));
			if(message_struct.data != NULL) free(message_struct.data);
			if(message_stream != NULL) free(message_stream);
			if(n < 0){
				error("\nError Sending over send to: ");
			}

			gettimeofday(&message_info_ptr->timestamp,NULL);
			MsgMap.insert(MSGMAPTYPE::value_type(curr_seq_num,message_info_ptr));

			queue_of_sent_seq.push_back(curr_seq_num);
		}
		pthread_mutex_unlock(&MsgMap_lock);
		select(0,NULL,NULL,NULL,&tvtime);
		tvtime.tv_sec = 0;
		tvtime.tv_usec = 0;
	}
	pthread_mutex_unlock(&MsgMap_lock);
	pthread_exit(NULL);
}
