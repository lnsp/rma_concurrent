/**
 * Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef DATA_STRUCTURES_DRIVER_HPP_
#define DATA_STRUCTURES_DRIVER_HPP_

#include <memory>
namespace distribution { class Interface; } // forward decl.

namespace data_structures {

void initialise();

void execute();

void prepare_parameters();

} // namespace data_structures

#endif /* DATA_STRUCTURES_DRIVER_HPP_ */
