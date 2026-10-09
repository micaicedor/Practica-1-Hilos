# Ejercicio 1 – Punto 2: muchas tareas, WORK, SPAN y speedup

Computación Paralela y Distribuida – Universidad Nacional de Colombia

**Integrantes:**
- Michael Sebastian Caicedo Rosero
- Sergio Andres Hernandez Salinas
- Juan David Castañeda

Este documento explica la actividad 2 (`parManyTaskArraySum`), los conceptos de
**WORK**, **SPAN** (ruta crítica) y **speedup** aplicados a este trabajo, y por qué
falla la 4.ª prueba (`testParManyTaskTwoHundredMillion`).

---

## 1. Qué se hizo (versión entregada)

### Punto 1 – `parArraySum` (2 tareas)

```java
left.fork();      // la mitad izquierda se envía al pool común (otro hilo)
right.compute();  // la mitad derecha se calcula en el hilo actual
left.join();      // se espera a que termine la izquierda
return left.getValue() + right.getValue();
```

Es el patrón clásico de Fork/Join: mientras otro hilo procesa la izquierda, el hilo
actual no se queda esperando, sino que procesa la derecha. Como `fork()` se llama
desde fuera de un pool, la tarea va al **pool común de Java**
(`ForkJoinPool.commonPool()`), que la JVM crea una sola vez y reutiliza.

### Punto 2 – `parManyTaskArraySum` (N tareas)

1. **Dividir:** se crea una `ReciprocalArraySumTask` por cada bloque del arreglo.
   El rango del bloque `i` lo calculan `getChunkStartInclusive(i, numTasks, n)`
   (dónde empieza, incluido) y `getChunkEndExclusive(i, numTasks, n)` (dónde
   termina, excluido; el último bloque se recorta si no da exacto).
2. **Ejecutar en paralelo:** `ForkJoinTask.invokeAll(tasks)` lanza todas las tareas
   y espera a que terminen. Internamente envía las tareas 1..N−1 al pool común y
   ejecuta la tarea 0 en el hilo actual, igual que el `fork/compute/join` del punto 1
   pero generalizado a N tareas.
3. **Combinar:** se suman los `getValue()` de todas las tareas.

El pool común tiene por defecto *núcleos − 1* hilos (15 en el equipo de 16 núcleos
donde se corrieron las pruebas finales); sumando el hilo que llama a `invokeAll`,
trabajan los 16 núcleos a la vez.

---

## 2. El grafo computacional

Un programa paralelo se puede dibujar como un grafo: cada nodo es un paso con un
costo (tiempo) y las flechas indican qué debe terminar antes de qué.

Si cada recíproco `1/x` cuesta 1 unidad y el arreglo tiene `n` elementos:

```
Secuencial:      inicio → 1/x0 → 1/x1 → 1/x2 → ... → 1/x(n-1) → fin

2 tareas:              ┌─ n/2 recíprocos ─┐
                inicio ┤                  ├─ suma final → fin
                       └─ n/2 recíprocos ─┘

N tareas:              ┌─ n/N recíprocos ─┐
                       ├─ n/N recíprocos ─┤
                inicio ┤       ...        ├─ suma de N parciales → fin
                       └─ n/N recíprocos ─┘
```

---

## 3. WORK, SPAN y paralelismo ideal

| Concepto | Definición | Qué significa |
|---|---|---|
| **WORK** | Suma de los costos de **todos** los nodos | Tiempo con **1** procesador (equivale al tiempo secuencial) |
| **SPAN** (CPL, ruta crítica) | Costo de la **ruta más larga** del grafo | Tiempo mínimo aunque hubiera **infinitos** procesadores |
| **Paralelismo ideal** | WORK ÷ SPAN | El speedup máximo que el algoritmo permite en teoría |

Modelo de costo usado: cada división `1/x` cuesta 1 unidad. Repartir el arreglo en
P tareas y combinar sus P sumas parciales al final cuesta P − 1 unidades adicionales
(una suma por cada resultado parcial que se junta). Con eso:

- **WORK** = n + (P − 1) → se hacen las n divisiones más las P − 1 sumas de combinación.
- **SPAN** = n/P + (P − 1) → la ruta más larga es: la tarea más cargada (n/P divisiones,
  todas en paralelo) y luego las P − 1 sumas de combinación, que son secuenciales
  porque cada una depende de la anterior.

### Cálculo real para cada prueba (n = tamaño del arreglo, P = número de tareas)

El equipo donde se corrieron las pruebas finales tiene **16 núcleos**, así que las
pruebas de muchas tareas usan P = 16.

| # | Prueba | n | P | WORK = n + (P−1) | SPAN = n/P + (P−1) | Paralelismo ideal = WORK/SPAN |
|---|---|---|---|---|---|---|
| 1 | `testParSimpleTwoMillion` | 2 000 000 | 2 | 2 000 001 | 1 000 001 | 1,99 |
| 2 | `testParSimpleTwoHundredMillion` | 200 000 000 | 2 | 200 000 001 | 100 000 001 | 1,99 |
| 3 | `testParManyTaskTwoMillion` | 2 000 000 | 16 | 2 000 015 | 125 015 | 15,99 |
| 4 | `testParManyTaskTwoHundredMillion` | 200 000 000 | 16 | 200 000 015 | 12 500 015 | 15,99 |

(El paralelismo ideal se **truncó hacia abajo**, no se redondeó hacia arriba: como
SPAN siempre incluye el costo de combinar las P sumas parciales, WORK/SPAN es
siempre un poco **menor** que P, nunca llega a P ni lo supera. Para n tan grande ese
costo de combinación es insignificante frente a n, por eso el resultado queda tan
cerca de P — 1,99 de 2 y 15,99 de 16 — pero formalmente nunca lo alcanza.)

**¿Por qué no usar una tarea por elemento si su paralelismo ideal sería enorme?**
Porque crear, programar y esperar una tarea cuesta mucho más que calcular un solo
`1/x`. Con 16 núcleos no se puede aprovechar un paralelismo de millones; basta con
unas pocas tareas grandes, una por núcleo.

### Límites del tiempo con P procesadores

- **T_P ≥ WORK / P**: el trabajo no se puede repartir mejor que en partes iguales.
- **T_P ≥ SPAN**: nunca se baja de la ruta crítica.

Por lo tanto:

> **speedup ≤ mín(P, WORK / SPAN)**

Con 2 tareas el techo es 2; con 16 tareas en 16 núcleos el techo es 16.

---

## 4. Speedup

```
speedup = tiempo secuencial ÷ tiempo paralelo
eficiencia = speedup ÷ número de núcleos
```

- Speedup 1,5 significa 1,5 veces más rápido: el paralelo tarda como máximo el
  66,7 % del tiempo secuencial (no la mitad).
- La prueba calcula el speedup ejecutando 60 veces cada versión y midiendo el
  tiempo en milisegundos.

### Lo que exige cada prueba (equipo con 16 núcleos)

| # | Prueba | Arreglo | Exige | Eficiencia exigida |
|---|---|---|---|---|
| 1 | `testParSimpleTwoMillion` | 2 M (16 MB) | 1,5 | – |
| 2 | `testParSimpleTwoHundredMillion` | 200 M (1,6 GB) | 1,5 | – |
| 3 | `testParManyTaskTwoMillion` | 2 M (16 MB) | 0,6 × 16 = 9,6 | 60 % |
| 4 | `testParManyTaskTwoHundredMillion` | 200 M (1,6 GB) | 0,8 × 16 = 12,8 | 80 % |

### Resultados obtenidos (ejecución final, `mvn test`)

| # | Prueba | Exigido | Obtenido | Estado |
|---|---|---|---|---|
| 1 | `testParSimpleTwoMillion` | ≥ 1,5 | ≥ 1,5 | ✅ pasa |
| 2 | `testParSimpleTwoHundredMillion` | ≥ 1,5 | ≥ 1,5 | ✅ pasa |
| 3 | `testParManyTaskTwoMillion` | ≥ 9,6 | ≥ 9,6 | ✅ pasa |
| 4 | `testParManyTaskTwoHundredMillion` | ≥ 12,8 | **2,32** | ❌ falla |

```
Tests run: 4, Failures: 1, Errors: 0, Skipped: 0
testParManyTaskTwoHundredMillion: "Se esperaba ... 12.800000x veces más rápido,
pero solo alcanzó a mejorar la rapidez (speedup) 2.318182x veces"
```

Las cuatro pruebas obtienen el **resultado numérico correcto**; solo la 4.ª falla, y
falla por el umbral de velocidad, no por un error de cálculo.

---

## 5. El problema de la 4.ª prueba

### Qué tiene de raro lo que exige

La 4.ª prueba exige **más eficiencia (80 %) al arreglo grande** que la 3.ª al
arreglo pequeño (60 %). Esa lógica supone que el único obstáculo del paralelismo es
el costo de crear y coordinar tareas, que con un arreglo enorme se vuelve
insignificante. **Pero ignora la memoria.**

### Por qué falla: el problema está limitado por memoria (*memory-bound*)

- **2 millones de elementos (16 MB)** caben total o casi totalmente en la caché del
  procesador, así que cada núcleo trabaja casi siempre sobre datos ya cercanos a él.
- **200 millones de elementos (1,6 GB)** no caben en ninguna caché. En cada pasada,
  los 1,6 GB deben leerse desde la RAM, y la RAM tiene un solo "canal" compartido por
  todos los núcleos.

Con un solo núcleo, la velocidad la limita la división (el núcleo pide datos más
despacio de lo que la RAM puede entregarlos). Pero cuando **16 núcleos piden datos a
la vez, el canal de memoria se satura** mucho antes de que los 16 núcleos puedan
trabajar a plena capacidad: agregar más núcleos deja de acelerar nada porque todos
esperan a la misma RAM.

Esto explica por qué la brecha es tan grande con 16 núcleos (se obtiene 2,32x contra
un techo teórico de 15,99x, y un mínimo exigido de 12,8x): mientras más núcleos
compiten por el mismo canal de memoria, más rápido se satura ese canal y menos ayuda
cada núcleo adicional.

### Por qué ningún cambio en Fork/Join lo arregla

Para sumar los recíprocos hay que **leer cada elemento al menos una vez**, así que los
1,6 GB tienen que pasar por la RAM sí o sí. Da igual usar `fork/join`, `invokeAll`
o un pool manual: todas las variantes leen la misma cantidad de memoria, así que
ninguna estrategia de reparto de tareas puede evitar el cuello de botella.

### Relación con WORK/SPAN

El modelo WORK/SPAN dice que el speedup máximo es mín(P, WORK/SPAN) ≈ 16, pero
**asume que leer memoria es gratis**. Este problema hace muy poco cálculo por cada
dato leído (una división), así que el cuello de botella real es el ancho de banda de
la memoria, no el número de núcleos disponibles. Por eso el speedup obtenido (2,32x)
queda muy por debajo del techo teórico (≈16x) aunque haya núcleos libres.

### Factores que además añaden variación

- La prueba mide tiempo real de reloj, sin calentar la JVM antes de medir (el JIT
  todavía está optimizando durante las primeras repeticiones).
- No hay margen de tolerancia: el umbral es un número fijo, así que pequeñas
  variaciones de carga del sistema pueden mover el resultado.
- Otros programas abiertos, el plan de energía y la temperatura del procesador
  (que baja su frecuencia con muchos núcleos ocupados) también influyen.
