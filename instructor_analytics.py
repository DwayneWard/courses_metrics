"""
Модуль для анализа успеваемости студентов по преподавателям и группам.
Поддерживает загрузку распределения студентов из Excel и расчет метрик по группам.
"""

import warnings
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from course_analytics import CourseAnalytics

warnings.filterwarnings('ignore')


class InstructorAnalytics:
    """Класс для анализа успеваемости по преподавателям и группам."""
    
    def __init__(
        self,
        course_analytics: CourseAnalytics,
        instructor_distribution_path: str,
        student_name_column: str = None,
        config: Optional[Dict] = None,
    ):
        """
        Инициализация анализатора по преподавателям.
        
        Parameters:
        -----------
        course_analytics : CourseAnalytics
            Экземпляр CourseAnalytics с загруженными данными курса
        instructor_distribution_path : str
            Путь к Excel файлу с распределением студентов по преподавателям
        student_name_column : str, optional
            Название столбца с именами студентов в ведомости (по умолчанию первый столбец)
        config : dict, optional
            Конфигурация для выбора активностей:
            - "activities_filter": "all" | "engagement" | List[str]
              - "all": все активности
              - "engagement": только engagement активности (по умолчанию)
              - List[str]: список конкретных названий активностей
        """
        self.course_analytics = course_analytics
        self.instructor_distribution_path = instructor_distribution_path
        self.student_name_column = student_name_column
        self.config = config or {}
        
        # Определяем фильтр активностей
        self.activities_filter = self.config.get("activities_filter", "engagement")
        
        # Загружаем распределение студентов
        self.instructor_distribution = self._load_instructor_distribution()
        
        # Создаем маппинг студент -> преподаватель -> группа
        self.student_to_instructor = self._build_student_mapping()
        
        # Получаем данные студентов из ведомости
        self._extract_student_names()
    
    def _get_filtered_works(self) -> Dict[str, Dict]:
        """
        Возвращает отфильтрованный список работ в зависимости от конфигурации.
        
        Returns:
        --------
        Dict[str, Dict] - словарь работ, прошедших фильтр
        """
        all_works = self.course_analytics.work_data
        
        if self.activities_filter == "all":
            return all_works
        elif self.activities_filter == "engagement":
            # Только engagement активности
            engagement_types = set(self.course_analytics.engagement_activity_types or [])
            filtered = {}
            for work_name, work_data in all_works.items():
                if work_data['work_type'] in engagement_types:
                    filtered[work_name] = work_data
            return filtered
        elif isinstance(self.activities_filter, list):
            # Конкретный список названий активностей
            filtered = {}
            for work_name in self.activities_filter:
                if work_name in all_works:
                    filtered[work_name] = all_works[work_name]
            return filtered
        else:
            # По умолчанию engagement
            engagement_types = set(self.course_analytics.engagement_activity_types or [])
            filtered = {}
            for work_name, work_data in all_works.items():
                if work_data['work_type'] in engagement_types:
                    filtered[work_name] = work_data
            return filtered
    
    def _load_instructor_distribution(self) -> pd.DataFrame:
        """Загружает файл с распределением студентов по преподавателям."""
        try:
            df = pd.read_excel(self.instructor_distribution_path)
            return df
        except Exception as e:
            raise Exception(f"Не удалось загрузить файл распределения: {e}")
    
    def _build_student_mapping(self) -> Dict[str, Dict]:
        """
        Строит маппинг: студент (email/имя) -> преподаватель -> группа.
        
        Returns:
        --------
        Dict[str, Dict] - {student_identifier: {'instructor': str, 'group': str}}
        """
        mapping = {}
        
        instructor_col = 'Преподаватель'
        if instructor_col not in self.instructor_distribution.columns:
            # Пробуем найти столбец с преподавателями
            possible_cols = [col for col in self.instructor_distribution.columns 
                           if 'преподаватель' in col.lower() or 'преп' in col.lower()]
            if possible_cols:
                instructor_col = possible_cols[0]
            else:
                raise ValueError("Не найден столбец с преподавателями")
        
        # Находим столбцы с группами студентов
        group_cols = [col for col in self.instructor_distribution.columns 
                     if col != instructor_col and ('студент' in col.lower() or 'группа' in col.lower())]
        
        if not group_cols:
            raise ValueError("Не найдены столбцы с группами студентов")
        
        for _, row in self.instructor_distribution.iterrows():
            instructor = str(row[instructor_col]).strip()
            if pd.isna(instructor) or instructor.lower() == 'nan':
                continue
            
            for group_col in group_cols:
                students_str = row[group_col]
                if pd.isna(students_str):
                    continue
                
                # Извлекаем номер группы из названия столбца
                group_num = self._extract_group_number(group_col)
                group_name = f"Группа {group_num}" if group_num else group_col
                
                # Разбиваем строку студентов (могут быть через запятую, точку с запятой и т.д.)
                students = self._parse_students_string(students_str)
                
                for student in students:
                    student_clean = self._normalize_student_name(student)
                    if student_clean:
                        mapping[student_clean] = {
                            'instructor': instructor,
                            'group': group_name,
                            'instructor_group': f"{instructor} - {group_name}"
                        }
        
        return mapping
    
    def _extract_group_number(self, group_col: str) -> Optional[int]:
        """Извлекает номер группы из названия столбца."""
        import re
        match = re.search(r'(\d+)', str(group_col))
        return int(match.group(1)) if match else None
    
    def _parse_students_string(self, students_str: str) -> List[str]:
        """Парсит строку со списком студентов."""
        if pd.isna(students_str):
            return []
        
        students_str = str(students_str).strip()
        # Разбиваем по запятой, точке с запятой или переносу строки
        students = []
        for delimiter in [',', ';', '\n', '\r\n']:
            if delimiter in students_str:
                students = [s.strip() for s in students_str.split(delimiter)]
                break
        
        if not students:
            students = [students_str]
        
        return [s for s in students if s and s.lower() != 'nan']
    
    def _normalize_student_name(self, student: str) -> Optional[str]:
        """
        Нормализует имя студента или email для сопоставления.
        Для email: оставляет полный email в нижнем регистре.
        Для имени: убирает лишние пробелы, приводит к нижнему регистру.
        """
        if not student or pd.isna(student):
            return None
        
        student = str(student).strip()
        if not student or student.lower() == 'nan':
            return None
        
        # Если это email, нормализуем его
        if '@' in student:
            # Оставляем полный email в нижнем регистре
            email = student.lower().strip()
            # Убираем лишние пробелы
            email = email.replace(' ', '')
            return email if email else None
        
        # Для имени: приводим к нижнему регистру и убираем лишние пробелы
        name = ' '.join(student.lower().split())
        return name if name else None
    
    def _extract_student_names(self):
        """Извлекает имена студентов из ведомости курса."""
        if self.course_analytics.students_df is None or len(self.course_analytics.students_df) == 0:
            raise ValueError("Нет данных о студентах в ведомости")
        
        # Первый столбец обычно содержит ФИО студентов
        student_col = self.student_name_column or self.course_analytics.students_df.columns[0]
        
        self.student_names_from_gradebook = []
        for idx, row in self.course_analytics.students_df.iterrows():
            student_full_info = str(row[student_col])
            # Извлекаем имя студента (до переноса строки или @)
            student_name = student_full_info.split('\n')[0].split('@')[0].strip()
            
            # Извлекаем email если есть
            email = None
            if '@' in student_full_info:
                # Ищем email в строке
                import re
                email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', student_full_info)
                if email_match:
                    email = email_match.group(0).lower().strip()
            
            # Нормализуем имя и email
            normalized_name = self._normalize_student_name(student_name)
            normalized_email = self._normalize_student_name(email) if email else None
            
            self.student_names_from_gradebook.append({
                'index': idx,
                'full_info': student_full_info,
                'name': student_name,
                'email': email,
                'normalized': normalized_name,
                'normalized_email': normalized_email
            })
    
    def _match_student_to_instructor(self, student_info: Dict) -> Optional[Dict]:
        """
        Находит преподавателя и группу для студента.
        
        Parameters:
        -----------
        student_info : Dict
            Словарь с информацией о студенте (normalized, normalized_email)
        
        Returns:
        --------
        Optional[Dict] - информация о преподавателе и группе
        """
        normalized_name = student_info.get('normalized')
        normalized_email = student_info.get('normalized_email')
        
        # Сначала пробуем по email (самый надежный способ)
        if normalized_email:
            # Прямое совпадение по email
            if normalized_email in self.student_to_instructor:
                return self.student_to_instructor[normalized_email]
            
            # Частичное совпадение по email
            for key, value in self.student_to_instructor.items():
                if normalized_email in key or key in normalized_email:
                    return value
        
        # Затем пробуем по имени
        if normalized_name:
            # Прямое совпадение по имени
            if normalized_name in self.student_to_instructor:
                return self.student_to_instructor[normalized_name]
            
            # Частичное совпадение по имени
            for key, value in self.student_to_instructor.items():
                if normalized_name in key or key in normalized_name:
                    return value
        
        return None
    
    def get_instructor_groups(self) -> pd.DataFrame:
        """
        Возвращает список всех преподавателей и их групп.
        
        Returns:
        --------
        pd.DataFrame с колонками: Преподаватель, Группа, Количество студентов
        """
        groups = []
        for student_info in self.student_names_from_gradebook:
            match = self._match_student_to_instructor(student_info)
            if match:
                groups.append({
                    'Преподаватель': match['instructor'],
                    'Группа': match['group'],
                    'Преподаватель-Группа': match['instructor_group']
                })
        
        if not groups:
            return pd.DataFrame()
        
        df = pd.DataFrame(groups)
        df = df.drop_duplicates()
        df = df.sort_values(['Преподаватель', 'Группа'])
        
        # Подсчитываем количество студентов в каждой группе
        student_counts = {}
        for student_info in self.student_names_from_gradebook:
            match = self._match_student_to_instructor(student_info)
            if match:
                key = match['instructor_group']
                student_counts[key] = student_counts.get(key, 0) + 1
        
        df['Количество студентов'] = df['Преподаватель-Группа'].map(student_counts)
        df = df.fillna(0)
        
        return df
    
    def get_student_instructor_mapping(self) -> pd.DataFrame:
        """
        Возвращает полный маппинг студентов на преподавателей и группы.
        
        Returns:
        --------
        pd.DataFrame с колонками: Студент, Преподаватель, Группа, Преподаватель-Группа
        """
        mapping = []
        for student_info in self.student_names_from_gradebook:
            match = self._match_student_to_instructor(student_info)
            if match:
                mapping.append({
                    'Студент': student_info['name'],
                    'Преподаватель': match['instructor'],
                    'Группа': match['group'],
                    'Преподаватель-Группа': match['instructor_group']
                })
            else:
                mapping.append({
                    'Студент': student_info['name'],
                    'Преподаватель': None,
                    'Группа': None,
                    'Преподаватель-Группа': None
                })
        
        return pd.DataFrame(mapping)
    
    def get_performance_by_instructor(self) -> pd.DataFrame:
        """
        Метрика: Средние баллы по успеваемости по преподавателям (по группам).
        
        Returns:
        --------
        pd.DataFrame с колонками: Преподаватель, Группа, Работа, Средний балл, Медиана, Количество студентов
        """
        results = []
        
        # Получаем отфильтрованные работы
        filtered_works = self._get_filtered_works()
        
        for work_name, work_data in filtered_works.items():
            scores = work_data['all_scores']
            
            # Группируем по преподавателям и группам
            instructor_group_scores = {}
            
            for idx, student_info in enumerate(self.student_names_from_gradebook):
                if idx >= len(scores):
                    continue
                
                score = scores.iloc[idx]
                if pd.isna(score) or score <= 0:
                    continue
                
                match = self._match_student_to_instructor(student_info)
                if match:
                    key = match['instructor_group']
                    if key not in instructor_group_scores:
                        instructor_group_scores[key] = {
                            'instructor': match['instructor'],
                            'group': match['group'],
                            'scores': []
                        }
                    instructor_group_scores[key]['scores'].append(score)
            
            # Рассчитываем метрики для каждой группы
            for key, data in instructor_group_scores.items():
                if len(data['scores']) > 0:
                    results.append({
                        'Преподаватель': data['instructor'],
                        'Группа': data['group'],
                        'Преподаватель-Группа': key,
                        'Работа': work_name,
                        'Средний балл': np.mean(data['scores']),
                        'Медиана': np.median(data['scores']),
                        'Количество студентов': len(data['scores']),
                        'Тип работы': work_data['work_type']
                    })
        
        return pd.DataFrame(results)
    
    def get_median_performance_by_instructor(self) -> pd.DataFrame:
        """
        Метрика: Медианные баллы по успеваемости по преподавателям (по группам).
        
        Returns:
        --------
        pd.DataFrame с колонками: Преподаватель, Группа, Работа, Медиана, Количество студентов
        """
        performance_df = self.get_performance_by_instructor()
        if len(performance_df) == 0:
            return pd.DataFrame()
        
        # Группируем и берем медиану
        median_df = performance_df.groupby(['Преподаватель', 'Группа', 'Работа']).agg({
            'Медиана': 'first',
            'Количество студентов': 'first',
            'Тип работы': 'first'
        }).reset_index()
        
        return median_df
    
    def get_cor_by_instructor(self) -> pd.DataFrame:
        """
        Метрика: COR (Completion Rate) по преподавателям (по группам).
        COR = процент студентов, выполнивших работу.
        
        Returns:
        --------
        pd.DataFrame с колонками: Преподаватель, Группа, Работа, COR, Выполнили, Всего
        """
        results = []
        
        # Получаем отфильтрованные работы
        filtered_works = self._get_filtered_works()
        
        for work_name, work_data in filtered_works.items():
            scores = work_data['all_scores']
            
            # Группируем по преподавателям и группам
            instructor_group_stats = {}
            
            for idx, student_info in enumerate(self.student_names_from_gradebook):
                if idx >= len(scores):
                    continue
                
                score = scores.iloc[idx]
                match = self._match_student_to_instructor(student_info)
                if match:
                    key = match['instructor_group']
                    if key not in instructor_group_stats:
                        instructor_group_stats[key] = {
                            'instructor': match['instructor'],
                            'group': match['group'],
                            'completed': 0,
                            'total': 0
                        }
                    
                    instructor_group_stats[key]['total'] += 1
                    if not pd.isna(score) and score > 0:
                        instructor_group_stats[key]['completed'] += 1
            
            # Рассчитываем COR для каждой группы
            for key, data in instructor_group_stats.items():
                if data['total'] > 0:
                    cor = (data['completed'] / data['total']) * 100
                    results.append({
                        'Преподаватель': data['instructor'],
                        'Группа': data['group'],
                        'Преподаватель-Группа': key,
                        'Работа': work_name,
                        'COR': cor,
                        'Выполнили': data['completed'],
                        'Всего': data['total'],
                        'Тип работы': work_data['work_type']
                    })
        
        return pd.DataFrame(results)
    
    def get_summary_by_instructor(self) -> pd.DataFrame:
        """
        Сводная статистика по преподавателям и группам.
        
        Returns:
        --------
        pd.DataFrame с обобщенными метриками по каждому преподавателю и группе
        """
        performance_df = self.get_performance_by_instructor()
        cor_df = self.get_cor_by_instructor()
        
        if len(performance_df) == 0:
            return pd.DataFrame()
        
        # Агрегируем по преподавателю и группе
        summary = performance_df.groupby(['Преподаватель', 'Группа', 'Преподаватель-Группа']).agg({
            'Средний балл': ['mean', 'median'],
            'Медиана': 'mean',
            'Количество студентов': 'first'
        }).reset_index()
        
        # Упрощаем названия колонок
        summary.columns = ['Преподаватель', 'Группа', 'Преподаватель-Группа', 
                          'Средний балл (среднее)', 'Средний балл (медиана)', 
                          'Медиана баллов', 'Количество студентов']
        
        # Добавляем средний COR
        if len(cor_df) > 0:
            avg_cor = cor_df.groupby(['Преподаватель', 'Группа'])['COR'].mean().reset_index()
            avg_cor.columns = ['Преподаватель', 'Группа', 'Средний COR']
            summary = summary.merge(avg_cor, on=['Преподаватель', 'Группа'], how='left')
        else:
            summary['Средний COR'] = np.nan
        
        return summary
    
    def get_all_metrics(self) -> Dict:
        """Возвращает все рассчитанные метрики по преподавателям."""
        return {
            'instructor_groups': self.get_instructor_groups(),
            'student_mapping': self.get_student_instructor_mapping(),
            'performance': self.get_performance_by_instructor(),
            'median_performance': self.get_median_performance_by_instructor(),
            'cor': self.get_cor_by_instructor(),
            'summary': self.get_summary_by_instructor()
        }

