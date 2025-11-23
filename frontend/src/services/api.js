import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const predictWithChance = async (data) => {
  const response = await api.post('/predict/chance', data);
  return response.data;
};

export const findColleges = async (data) => {
  const response = await api.post('/colleges/find', data);
  return response.data;
};

export const getCollegesList = async (examType = 'CET', collegeCode = null) => {
  let url = `/colleges/list?exam_type=${examType}`;
  if (collegeCode) {
    url += `&college_code=${collegeCode}`;
  }
  const response = await api.get(url);
  return response.data;
};

export default api;
